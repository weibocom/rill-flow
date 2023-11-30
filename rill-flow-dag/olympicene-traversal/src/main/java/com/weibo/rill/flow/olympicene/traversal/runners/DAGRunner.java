/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.olympicene.traversal.runners;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.helper.DAGInfoMaker;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.olympicene.core.model.DAGSettings;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg.ExecutionInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.helper.Stasher;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping;
import com.weibo.rill.flow.interfaces.model.task.*;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class DAGRunner {
    private final DAGContextStorage dagContextStorage;
    private final DAGInfoStorage dagInfoStorage;
    private final DAGStorageProcedure dagStorageProcedure;
    @Setter
    private Stasher stasher;

    public DAGRunner(DAGContextStorage dagContextStorage, DAGInfoStorage dagInfoStorage, DAGStorageProcedure dagStorageProcedure) {
        this.dagContextStorage = dagContextStorage;
        this.dagInfoStorage = dagInfoStorage;
        this.dagStorageProcedure = dagStorageProcedure;
    }

    public ExecutionResult submitDAG(String executionId, DAG dag, DAGSettings settings, Map<String, Object> data, NotifyInfo notifyInfo) {
        ExecutionResult ret = ExecutionResult.builder().build();

        dagStorageProcedure.lockAndRun(LockerKey.buildDagInfoLockName(executionId), () -> {
            DAGInfo currentExecutionIdDagInfo = dagInfoStorage.getBasicDAGInfo(executionId);
            submitValidate(executionId, dag, settings.isIgnoreExist(), currentExecutionIdDagInfo);

            // 任务执行过程中会频繁获取DAG
            // 理论上对默认context大小不做限制 默认context存入存储后 dag中定义的默认存储在后续逻辑中不会使用
            // 为减少后续获DAG的大小从而减少网络开销 此处将默认context设置为null
            Map<String, Object> context = Maps.newHashMap();
            Map<String, String> defaultContext = Optional.ofNullable(dag.getDefaultContext()).orElse(Collections.emptyMap());
            defaultContext.forEach((key, value) -> context.put(key, JSONPathInputOutputMapping.parseSource(value)));
            Optional.ofNullable(data).ifPresent(context::putAll);
            ret.setContext(context);
            dag.setDefaultContext(null);

            // inputMapping/outputMapping中可能存在引用通用mapping的情况
            // 此处将引用替换为实际内容
            // 后续处理中存在只获取TaskInfo不获取DAG的情况 所以若不替换 则需要在每个获取TaskInfo的地方同时获取DAG
            // 采用空间换时间的策略 此处将引用替换为实际内容后 存储DAG的大小会变大 但可以简化后续处理的复杂度 不用每次都获取DAG
            // mapping中引用替换为实际内容后 后续处理将不再使用commonMapping 为减少DAG大小 此处将commonMapping设置为null
            handleMappingReference(1, dag.getCommonMapping(), dag.getTasks());
            handleMappingReference(dag.getCommonMapping(), dag.getCallbackConfig());
            dag.setCommonMapping(null);

            Optional.ofNullable(dag.getResources()).ifPresent(resources ->
                    handleResources(1, resources.stream().collect(Collectors.toMap(BaseResource::getName, it -> it)), dag.getTasks()));
            dag.setResources(null);

            DAGInvokeMsg dagInvokeMsg = buildInvokeMsg(executionId, settings, notifyInfo);
            DAGInfo dagInfoToUpdate = new DAGInfoMaker()
                    .dag(dag)
                    .executionId(executionId)
                    .dagInvokeMsg(dagInvokeMsg)
                    .dagStatus(DAGStatus.RUNNING)
                    .make();
            ret.setDagInfo(dagInfoToUpdate);
            Optional.ofNullable(dagInvokeMsg)
                    .map(DAGInvokeMsg::getExecutionRoutes)
                    .filter(CollectionUtils::isNotEmpty)
                    .map(it -> it.get(0))
                    .map(ExecutionInfo::getExecutionId)
                    .filter(StringUtils::isNotBlank)
                    .ifPresent(rootExecutionId -> context.putIfAbsent("flow_root_execution_id", rootExecutionId));

            dagContextStorage.updateContext(executionId, context);
            dagInfoStorage.saveDAGInfo(executionId, dagInfoToUpdate);
        });

        return ret;
    }

    private void handleResources(int currentDepth, Map<String, BaseResource> resourceMap, List<BaseTask> tasks) {
        if (MapUtils.isEmpty(resourceMap) || CollectionUtils.isEmpty(tasks)) {
            return;
        }

        if (currentDepth > SystemConfig.getTaskMaxDepth()) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), "exceed max depth");
        }

        tasks.stream()
                .peek(task -> handleResources(currentDepth + 1, resourceMap, task.subTasks()))
                .filter(task -> task instanceof FunctionTask)
                .map(task -> (FunctionTask) task)
                .filter(functionTask -> functionTask.getResource() == null)
                .filter(functionTask -> StringUtils.isNotBlank(functionTask.getResourceName()))
                .forEach(functionTask -> {
                    String[] values = functionTask.getResourceName().split(ReservedConstant.FUNCTION_TASK_RESOURCE_NAME_SCHEME_CONNECTOR);
                    if (values.length != 2 || !"resource".equals(values[0])) {
                        return;
                    }
                    BaseResource resource = Optional.ofNullable(resourceMap.get(values[1]))
                            .orElseThrow(() -> {
                                int code = TraversalErrorCode.DAG_ILLEGAL_STATE.getCode();
                                String msg = functionTask.getName() + " can not find task resource " + functionTask.getResourceName();
                                return new DAGTraversalException(code, msg);
                            });
                    functionTask.setResource(resource);
                });
    }

    private void handleMappingReference(Map<String, List<Mapping>> commonMapping, CallbackConfig callbackConfig) {
        List<Mapping> callbackInputMappings = Optional.ofNullable(callbackConfig)
                .map(CallbackConfig::getInputMappings).orElse(null);
        if (MapUtils.isEmpty(commonMapping) || CollectionUtils.isEmpty(callbackInputMappings)) {
            return;
        }

        callbackConfig.setInputMappings(includeReferenceMappings(commonMapping, callbackInputMappings));
    }

    private void handleMappingReference(int currentDepth, Map<String, List<Mapping>> commonMapping, List<BaseTask> tasks) {
        if (MapUtils.isEmpty(commonMapping) || CollectionUtils.isEmpty(tasks)) {
            return;
        }

        if (currentDepth > SystemConfig.getTaskMaxDepth()) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), "exceed max depth");
        }

        tasks.forEach(task -> {
            task.setInputMappings(includeReferenceMappings(commonMapping, task.getInputMappings()));
            task.setOutputMappings(includeReferenceMappings(commonMapping, task.getOutputMappings()));
            handleMappingReference(currentDepth + 1, commonMapping, task.subTasks());
        });
    }

    private List<Mapping> includeReferenceMappings(Map<String, List<Mapping>> commonMapping, List<Mapping> taskMappings) {
        if (CollectionUtils.isEmpty(taskMappings)) {
            return taskMappings;
        }

        List<Mapping> includeReference = Lists.newArrayList();
        for (Mapping mapping : taskMappings) {
            if (StringUtils.isBlank(mapping.getReference())) {
                includeReference.add(mapping);
            } else {
                Optional.ofNullable(commonMapping.get(mapping.getReference())).ifPresent(includeReference::addAll);
            }
        }
        return includeReference;
    }

    private DAGInvokeMsg buildInvokeMsg(String executionId, DAGSettings settings, NotifyInfo notifyInfo) {
        InvokeTimeInfo invokeTimeInfo = InvokeTimeInfo.builder().startTimeInMillisecond(System.currentTimeMillis()).build();
        CallbackConfig callbackConfig = Optional.ofNullable(notifyInfo).map(NotifyInfo::getCallbackConfig).orElse(null);
        DAGInvokeMsg dagInvokeMsg = DAGInvokeMsg.builder().invokeTimeInfos(Lists.newArrayList(invokeTimeInfo)).callbackConfig(callbackConfig).build();

        if (notifyInfo == null || StringUtils.isBlank(notifyInfo.getParentDAGExecutionId())) {
            return dagInvokeMsg;
        }

        DAGInfo parentDAGInfo = dagInfoStorage.getBasicDAGInfo(notifyInfo.getParentDAGExecutionId());
        List<ExecutionInfo> parentDAGExecutionRoutes = Optional.ofNullable(parentDAGInfo.getDagInvokeMsg())
                .map(DAGInvokeMsg::getExecutionRoutes).orElse(new ArrayList<>());

        List<ExecutionInfo> currentDAGExecutionRoutes = Lists.newArrayList(parentDAGExecutionRoutes);
        currentDAGExecutionRoutes.add(ExecutionInfo.builder().index(parentDAGExecutionRoutes.size() + 1)
                .executionId(notifyInfo.getParentDAGExecutionId()).taskInfoName(notifyInfo.getParentDAGTaskInfoName())
                .executionType(notifyInfo.getParentDAGTaskExecutionType()).build());
        if (currentDAGExecutionRoutes.size() >= settings.getDagMaxDepth()) {
            String route = currentDAGExecutionRoutes.stream().sorted(Comparator.comparingInt(ExecutionInfo::getIndex))
                    .map(executionInfo -> executionInfo.getExecutionId() + "#" + executionInfo.getTaskInfoName())
                    .collect(Collectors.joining("->"));
            log.warn("submitDAG exceed max dag depth, executionId:{}, maxDAGDepth:{}, route:{}", executionId, settings.getDagMaxDepth(), route);
            throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "exceed max depth, dag route: " + route);
        }

        dagInvokeMsg.setExecutionRoutes(currentDAGExecutionRoutes);
        return dagInvokeMsg;
    }

    private void submitValidate(String executionId, DAG dag, boolean ignoreExists, DAGInfo dagInfo) {
        if (dagInfo != null && !ignoreExists) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ALREADY_EXIST.getCode(), "dag info " + executionId + " already exists");
        }
        if (dag == null) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_EXECUTION_NOT_FOUND.getCode(), "dag not found");
        }
    }

    public ExecutionResult finishDAG(String executionId, DAGInfo dagInfo, DAGStatus dagStatus, DAGInvokeMsg dagInvokeMsg) {
        log.info("finishDAG action start, executionId:{}, dagStatus:{}", executionId, dagStatus);
        if (dagInfo == null) {
            dagInfo = dagInfoStorage.getBasicDAGInfo(executionId);
        }

        dagInfo.setDagStatus(dagStatus);
        dagInfo.updateInvokeMsg();
        Map<String, TaskInfo> tasks = dagInfo.getTasks();
        dagInfo.setTasks(new LinkedHashMap<>());
        dagInfo.updateInvokeMsg(dagInvokeMsg);
        updateDAGInvokeEndTime(dagInfo);
        dagInfoStorage.saveDAGInfo(executionId, dagInfo);
        dagInfo.setTasks(tasks);

        DAGInfo wholeDagInfo = dagInfoStorage.getDAGInfo(executionId);
        Map<String, Object> context = dagContextStorage.getContext(executionId);

        // 存储上支持调用clear后过一段时间再删除
        // 为保证这段时间内状态正确 需要上述步骤中save操作
        // finishDAG在dag完成时才调用 qps相对其他操作低
        dagInfoStorage.clearDAGInfo(executionId);
        dagContextStorage.clearContext(executionId);

        if (stasher.needStashFlow(dagInfo, dagStatus)) {
            stasher.stashFlow(wholeDagInfo, context);
        }

        log.info("finishDAG finish, executionId:{}", executionId);
        return ExecutionResult.builder().dagInfo(wholeDagInfo).context(context).build();
    }

    private void updateDAGInvokeStartTime(DAGInfo dagInfo) {
        List<InvokeTimeInfo> invokeTimeInfos = getInvokeTimeInfoList(dagInfo);
        InvokeTimeInfo invokeTimeInfo = InvokeTimeInfo.builder().startTimeInMillisecond(System.currentTimeMillis()).build();
        invokeTimeInfos.add(invokeTimeInfo);
    }

    private void updateDAGInvokeEndTime(DAGInfo dagInfo) {
        List<InvokeTimeInfo> invokeTimeInfos = getInvokeTimeInfoList(dagInfo);
        if (CollectionUtils.isNotEmpty(invokeTimeInfos)) {
            invokeTimeInfos.get(invokeTimeInfos.size() - 1).setEndTimeInMillisecond(System.currentTimeMillis());
        }
    }

    private List<InvokeTimeInfo> getInvokeTimeInfoList(DAGInfo dagInfo) {
        DAGInvokeMsg dagInvokeMsg = Optional.ofNullable(dagInfo.getDagInvokeMsg()).orElseGet(() -> {
            dagInfo.setDagInvokeMsg(new DAGInvokeMsg());
            return dagInfo.getDagInvokeMsg();
        });

        return Optional.ofNullable(dagInvokeMsg.getInvokeTimeInfos()).orElseGet(() -> {
            dagInvokeMsg.setInvokeTimeInfos(Lists.newArrayList());
            return dagInvokeMsg.getInvokeTimeInfos();
        });
    }

    public void resetTask(String executionId, List<String> taskNames, Map<String, Object> data) {
        dagStorageProcedure.lockAndRun(LockerKey.buildDagInfoLockName(executionId), () -> {
            DAGInfo dagInfo = dagInfoStorage.getDAGInfo(executionId);
            if (dagInfo == null) {
                throw new DAGTraversalException(TraversalErrorCode.DAG_NOT_FOUND.getCode(), "can not find dag info");
            }

            List<TaskInfo> redoTaskInfos = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(taskNames)) {
                taskNames.stream()
                        .filter(StringUtils::isNotBlank)
                        .map(taskName -> DAGWalkHelper.getInstance().getAncestorTaskName(taskName))
                        .distinct()
                        .map(taskName -> dagInfo.getTasks().get(taskName))
                        .filter(Objects::nonNull)
                        .filter(taskInfo -> taskInfo.getTaskStatus() != TaskStatus.NOT_STARTED)
                        .forEach(redoTaskInfos::add);
            } else {
                dagInfo.getTasks().values().stream()
                        .filter(taskInfo -> taskInfo.getTaskStatus() != TaskStatus.NOT_STARTED)
                        .filter(taskInfo -> !taskInfo.getTaskStatus().isSuccessOrSkip())
                        .forEach(redoTaskInfos::add);
            }

            Optional.of(redoTaskInfos)
                    .filter(CollectionUtils::isNotEmpty)
                    .ifPresent(redoTasks -> {
                        if (dagInfo.getDagStatus().isCompleted()) {
                            updateDAGInvokeStartTime(dagInfo);
                        }
                        dagInfo.setDagStatus(DAGStatus.RUNNING);
                        resetTaskStatus(1, redoTasks);
                        dagInfoStorage.clearDAGInfo(executionId, 0);
                        dagInfoStorage.saveDAGInfo(executionId, dagInfo);
                    });
            Optional.ofNullable(data)
                    .filter(MapUtils::isNotEmpty)
                    .ifPresent(it -> dagContextStorage.updateContext(executionId, it));
        });
    }

    private void resetTaskStatus(int length, List<TaskInfo> next) {
        if (CollectionUtils.isEmpty(next) || length > 1000) {
            return;
        }

        next.forEach(taskInfo -> {
            taskInfo.setTaskStatus(TaskStatus.NOT_STARTED);
            taskInfo.setChildren(new LinkedHashMap<>());
            taskInfo.setSubGroupIndexToStatus(null);
            taskInfo.setSubGroupIndexToIdentity(null);
            resetTaskStatus(length + 1, taskInfo.getNext());
        });
    }
}
