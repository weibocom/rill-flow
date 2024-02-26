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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.*;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.strategy.Degrade;
import com.weibo.rill.flow.interfaces.model.strategy.Progress;
import com.weibo.rill.flow.interfaces.model.task.*;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.ReturnTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.storage.redis.lock.ResourceLoader;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Slf4j
public abstract class AbstractTaskRunner implements TaskRunner {
    private static final String NORMAL_SKIP_MSG = "skip due to dependent tasks return or degrade";
    public static final String EXPECTED_COST = "expected_cost";

    protected final Configuration valuePathConf = Configuration.builder()
            .options(Option.SUPPRESS_EXCEPTIONS)
            .options(Option.AS_PATH_LIST)
            .build();

    protected final InputOutputMapping inputOutputMapping;
    protected final DAGInfoStorage dagInfoStorage;
    protected final DAGContextStorage dagContextStorage;
    protected final DAGStorageProcedure dagStorageProcedure;
    protected final SwitcherManager switcherManager;

    public AbstractTaskRunner(InputOutputMapping inputOutputMapping, DAGInfoStorage dagInfoStorage,
                              DAGContextStorage dagContextStorage, DAGStorageProcedure dagStorageProcedure,
                              SwitcherManager switcherManager) {
        this.inputOutputMapping = inputOutputMapping;
        this.dagInfoStorage = dagInfoStorage;
        this.dagContextStorage = dagContextStorage;
        this.dagStorageProcedure = dagStorageProcedure;
        this.switcherManager = switcherManager;
    }

    public String getIcon() {
        return "";
    }

    public JSONObject getFields() {
        TaskCategory category = getCategory();
        try {
            String config = ResourceLoader.loadResourceAsText("metadata/fields/" + category.getValue() + ".json");
            return JSON.parseObject(config);
        } catch (Exception e) {
            log.warn("get fields error, category: {}", category.getValue(), e);
            throw new RuntimeException(e);
        }
    }

    public abstract TaskCategory getCategory();

    public boolean isEnable() {
        return true;
    }

    protected abstract ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input);

    @Override
    public void inputMappings(Map<String, Object> context, Map<String, Object> input, Map<String, Object> output, List<Mapping> rules) {
        inputOutputMapping.mapping(context, input, output, rules);
    }

    @Override
    public ExecutionResult run(String executionId, TaskInfo taskInfo, Map<String, Object> context) {
        try {
            if (needNormalSkip(executionId, taskInfo)) {
                skipCurrentAndFollowingTasks(executionId, taskInfo);
                return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
            }

            updateTaskInvokeStartTime(taskInfo);
            Map<String, Object> input = inputMappingCalculate(executionId, taskInfo, context);

            if (switcherManager.getSwitcherState("ENABLE_SET_INPUT_OUTPUT")) {
                taskInfo.getTaskInvokeMsg().setInput(input);
                updateTaskExpectedCost(taskInfo, input);
            }

            long taskSuspenseTime = getSuspenseInterval(executionId, taskInfo, input);
            if (taskSuspenseTime > 0) {
                log.info("task need wait, executionId:{}, taskName:{}, suspenseTime:{}", executionId, taskInfo.getName(), taskSuspenseTime);
                dagInfoStorage.saveTaskInfos(executionId, Sets.newHashSet(taskInfo));
                return ExecutionResult.builder().needRetry(true).retryIntervalInSeconds((int) taskSuspenseTime)
                        .taskStatus(taskInfo.getTaskStatus()).taskInfo(taskInfo).context(context).build();
            }

            degradeTasks(executionId, taskInfo);
            if (taskInfo.getTaskStatus().isCompleted()) {
                return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
            }

            updateProgressArgs(executionId, taskInfo, input);

            ExecutionResult ret = doRun(executionId, taskInfo, input);
            if (MapUtils.isEmpty(ret.getInput())) {
                ret.setInput(input);
            }
            return ret;
        } catch (Exception e) {
            log.warn("run task fails, executionId:{}, taskName:{}", executionId, taskInfo.getName(), e);

            if (!Optional.ofNullable(taskInfo.getTaskInvokeMsg()).map(TaskInvokeMsg::getMsg).isPresent()) {
                taskInfo.updateInvokeMsg(TaskInvokeMsg.builder().msg(e.getMessage()).build());
            }
            updateTaskInvokeEndTime(taskInfo);

            boolean tolerance = Optional.ofNullable(taskInfo.getTask()).map(BaseTask::isTolerance).orElse(false);
            taskInfo.setTaskStatus(tolerance ? TaskStatus.SKIPPED : TaskStatus.FAILED);

            Map<String, TaskInfo> subTasks = taskInfo.getChildren();
            taskInfo.setChildren(new LinkedHashMap<>());
            dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            taskInfo.setChildren(subTasks);

            return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
        }
    }

    private void skipCurrentAndFollowingTasks(String executionId, TaskInfo taskInfo) {
        log.info("skipCurrentAndFollowingTasks executionId:{} taskName:{}", executionId, taskInfo.getName());
        taskInfo.setTaskStatus(TaskStatus.SKIPPED);
        taskInfo.updateInvokeMsg(TaskInvokeMsg.builder().msg(NORMAL_SKIP_MSG).build());
        Set<TaskInfo> taskInfosNeedToUpdate = Sets.newHashSet();
        skipFollowingTasks(executionId, taskInfo, taskInfosNeedToUpdate);
        taskInfosNeedToUpdate.add(taskInfo);
        dagInfoStorage.saveTaskInfos(executionId, taskInfosNeedToUpdate);
    }

    /**
     * normalSkipTask: 因return任务条件满足或降级，导致后续被跳过的任务
     */
    private boolean needNormalSkip(String executionId, TaskInfo taskInfo) {
        try {
            List<TaskInfo> dependentTasks = taskInfo.getDependencies();
            return CollectionUtils.isNotEmpty(dependentTasks) &&
                    dependentTasks.stream()
                            .allMatch(dependentTask -> {
                                if ((dependentTask.getTask() instanceof ReturnTask) &&
                                        dependentTask.getTaskStatus() == TaskStatus.SUCCEED) {
                                    return true;
                                }
                                if (Optional.ofNullable(dependentTask.getTask()).map(BaseTask::getDegrade).map(Degrade::getFollowings).orElse(false)) {
                                    return true;
                                }
                                return dependentTask.getTaskStatus() == TaskStatus.SKIPPED &&
                                        Optional.ofNullable(dependentTask.getTaskInvokeMsg()).map(TaskInvokeMsg::getMsg).map(NORMAL_SKIP_MSG::equals).orElse(false);
                            });
        } catch (Exception e) {
            log.warn("needSkip fails, executionId:{} taskName:{} errorMsg:{}", executionId, taskInfo.getName(), e.getMessage());
            return false;
        }
    }

    private void updateProgressArgs(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        try {
            List<Mapping> args = Optional.ofNullable(taskInfo.getTask())
                    .map(BaseTask::getProgress)
                    .map(Progress::getArgs)
                    .orElse(null);
            if (CollectionUtils.isEmpty(args)) {
                return;
            }

            List<Mapping> mappings = args.stream()
                    .map(it -> new Mapping(it.getSource(), "$.output." + it.getVariable()))
                    .toList();

            Map<String, Object> output = Maps.newHashMap();
            inputMappings(new HashMap<>(), input, output, mappings);
            taskInfo.getTaskInvokeMsg().setProgressArgs(output);
        } catch (Exception e) {
            log.warn("updateProgressArgs fails, executionId:{}, taskName:{}, errorMsg:{}", executionId, taskInfo.getName(), e.getMessage());
        }
    }

    private long getSuspenseInterval(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        try {
            List<Mapping> timeMappings = Lists.newArrayList();
            Optional.ofNullable(taskInfo.getTask()).map(BaseTask::getTimeline).ifPresent(timeline -> {
                if (StringUtils.isNotBlank(timeline.getSuspenseTimestamp())) {
                    timeMappings.add(new Mapping(timeline.getSuspenseTimestamp(), "$.output.timestamp"));
                } else if (StringUtils.isNotBlank(timeline.getSuspenseIntervalSeconds())) {
                    timeMappings.add(new Mapping(timeline.getSuspenseIntervalSeconds(), "$.output.interval"));
                }
            });
            if (CollectionUtils.isEmpty(timeMappings)) {
                return 0;
            }

            Map<String, Object> output = Maps.newHashMap();
            inputMappings(new HashMap<>(), input, output, timeMappings);

            long taskInvokeTime = Optional.ofNullable(output.get("timestamp")).map(String::valueOf).map(Long::valueOf)
                    .orElse(Optional.ofNullable(output.get("interval"))
                            .map(intervalSeconds -> {
                                List<InvokeTimeInfo> invokeTimeInfos = taskInfo.getTaskInvokeMsg().getInvokeTimeInfos();
                                long taskStartTime = invokeTimeInfos.get(invokeTimeInfos.size() - 1).getStartTimeInMillisecond();
                                return taskStartTime + Long.parseLong(String.valueOf(intervalSeconds)) * 1000;
                            }).orElse(0L));
            return (taskInvokeTime - System.currentTimeMillis()) / 1000;
        } catch (Exception e) {
            log.warn("taskNeedSuspense fails, executionId:{}, taskName:{}, errorMsg:{}", executionId, taskInfo.getName(), e.getMessage());
            return -1L;
        }
    }

    private void degradeTasks(String executionId, TaskInfo taskInfo) {
        Set<TaskInfo> skippedTasks = Sets.newHashSet();
        Optional.ofNullable(taskInfo.getTask())
                .map(BaseTask::getDegrade)
                .map(Degrade::getFollowings)
                .filter(it -> it)
                .ifPresent(it -> {
                    log.info("run degrade strong dependency following tasks, executionId:{}, taskName:{}", executionId, taskInfo.getName());
                    skipFollowingTasks(executionId, taskInfo, skippedTasks);
                });
        Optional.ofNullable(taskInfo.getTask())
                .map(BaseTask::getDegrade)
                .map(Degrade::getCurrent)
                .filter(it -> it)
                .ifPresent(it -> {
                    log.info("run degrade task, executionId:{}, taskName:{}", executionId, taskInfo.getName());
                    taskInfo.setTaskStatus(TaskStatus.SKIPPED);
                    skippedTasks.add(taskInfo);
                });

        if (CollectionUtils.isNotEmpty(skippedTasks)) {
            dagInfoStorage.saveTaskInfos(executionId, skippedTasks);
        }
    }

    @Override
    public ExecutionResult finish(String executionId, NotifyInfo notifyInfo, Map<String, Object> output) {
        throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "task do not support finish action");
    }

    @Override
    public void outputMappings(Map<String, Object> context, Map<String, Object> input, Map<String, Object> output, List<Mapping> rules) {
        inputOutputMapping.mapping(context, input, output, rules);
    }

    private Map<String, Object> inputMappingCalculate(String executionId, TaskInfo taskInfo, Map<String, Object> context) {
        if (context == null) {
            return Maps.newHashMap();
        }

        try {
            Map<String, Object> input = Maps.newHashMap();
            inputMappings(context, input, new HashMap<>(), taskInfo.getTask().getInputMappings());
            return input;
        } catch (Exception e) {
            log.warn("inputMappingCalculate fails, executionId={}, taskName={}", executionId, taskInfo.getName(), e);
            throw new DAGTraversalException(TraversalErrorCode.TRAVERSAL_FAILED.getCode(), e.getMessage());
        }
    }

    protected void updateTaskInvokeStartTime(TaskInfo taskInfo) {
        List<InvokeTimeInfo> invokeTimeInfos = getInvokeTimeInfoList(taskInfo);
        if (invokeTimeInfos.isEmpty() ||
                invokeTimeInfos.get(invokeTimeInfos.size() - 1).getEndTimeInMillisecond() != null) {
            InvokeTimeInfo invokeTimeInfo = InvokeTimeInfo.builder().startTimeInMillisecond(System.currentTimeMillis()).build();
            invokeTimeInfos.add(invokeTimeInfo);
        }
    }

    protected void updateTaskExpectedCost(TaskInfo taskInfo, Map<String, Object> input) {
        List<InvokeTimeInfo> invokeTimeInfos = getInvokeTimeInfoList(taskInfo);
        if (CollectionUtils.isNotEmpty(invokeTimeInfos)) {
            Long expectedCost = buildExpectedCostByTaskInfo(input);
            Optional.ofNullable(expectedCost).ifPresent(
                    cost -> invokeTimeInfos.get(invokeTimeInfos.size() - 1).setExpectedCostInMillisecond(expectedCost)
            );
        }
    }

    private Long buildExpectedCostByTaskInfo(Map<String, Object> input) {
        return Optional.ofNullable(input.get(EXPECTED_COST))
                .filter(Long.class::isInstance)
                .map(Long.class::cast)
                .orElse(null);
    }

    protected void updateTaskInvokeEndTime(TaskInfo taskInfo) {
        List<InvokeTimeInfo> invokeTimeInfos = getInvokeTimeInfoList(taskInfo);
        if (CollectionUtils.isNotEmpty(invokeTimeInfos)) {
            invokeTimeInfos.get(invokeTimeInfos.size() - 1).setEndTimeInMillisecond(System.currentTimeMillis());
        }
    }

    private List<InvokeTimeInfo> getInvokeTimeInfoList(TaskInfo taskInfo) {
        TaskInvokeMsg taskInvokeMsg = Optional.ofNullable(taskInfo.getTaskInvokeMsg()).orElseGet(() -> {
            taskInfo.setTaskInvokeMsg(new TaskInvokeMsg());
            return taskInfo.getTaskInvokeMsg();
        });

        return Optional.ofNullable(taskInvokeMsg.getInvokeTimeInfos()).orElseGet(() -> {
            taskInvokeMsg.setInvokeTimeInfos(Lists.newArrayList());
            return taskInvokeMsg.getInvokeTimeInfos();
        });
    }

    protected void saveContext(String executionId, Map<String, Object> context, Set<TaskInfo> taskInfos) {
        Map<String, Object> contextToUpdate = Maps.newConcurrentMap();
        taskInfos.forEach(taskInfo -> {
            if (DAGWalkHelper.getInstance().isAncestorTask(taskInfo.getName())) {
                contextToUpdate.putAll(context);
            } else {
                contextToUpdate.put(DAGWalkHelper.getInstance().buildSubTaskContextFieldName(taskInfo.getRouteName()), context);
            }
        });
        dagContextStorage.updateContext(executionId, contextToUpdate);
    }

    protected ExecutionResult finishParentTask(String executionId, NotifyInfo notifyInfo) {
        log.info("finishParentTask actions start executionId:{} notifyInfo:{}", executionId, notifyInfo);
        ExecutionResult executionResult = ExecutionResult.builder().build();
        dagStorageProcedure.lockAndRun(LockerKey.buildTaskInfoLockName(executionId, notifyInfo.getTaskInfoName()), () -> {
            validateDAGInfo(executionId);
            TaskInfo taskInfo = dagInfoStorage.getBasicTaskInfo(executionId, notifyInfo.getTaskInfoName());
            finishActionValidateTaskInfo(executionId, notifyInfo.getTaskInfoName(), taskInfo);

            boolean groupStatusChanged = false;
            String completedGroupIndex = notifyInfo.getCompletedGroupIndex();
            TaskStatus groupTaskStatus = notifyInfo.getGroupTaskStatus();
            if (!groupTaskStatus.equals(taskInfo.getSubGroupIndexToStatus().get(completedGroupIndex))) {
                taskInfo.getSubGroupIndexToStatus().put(completedGroupIndex, groupTaskStatus);
                groupStatusChanged = true;
            }
            if (groupTaskStatus.isFailed()) {
                Optional.ofNullable(DAGWalkHelper.getInstance().getFailedTasks(notifyInfo.getTasks()))
                        .filter(CollectionUtils::isNotEmpty)
                        .map(it -> it.get(0))
                        .map(TaskInfo::getTaskInvokeMsg)
                        .ifPresent(failedTaskInvokeMsg -> {
                            TaskInvokeMsg update = failedTaskInvokeMsg.copy();
                            update.setInvokeTimeInfos(getInvokeTimeInfoList(taskInfo));
                            taskInfo.setTaskInvokeMsg(update);
                        });
            }

            TaskStatus taskStatus = DAGWalkHelper.getInstance().calculateParentStatus(taskInfo);
            if (taskStatus.isCompleted()) {
                log.info("finishParentTask begin to collect executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
                taskInfo.setTaskStatus(taskStatus);
                updateTaskInvokeEndTime(taskInfo);

                Map<String, Object> subTaskContext = getSubTaskContextMap(executionId, taskInfo);
                Map<String, Object> parentTaskContext = ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);
                if (MapUtils.isNotEmpty(subTaskContext) && CollectionUtils.isNotEmpty(taskInfo.getTask().getOutputMappings())) {
                    outputMappings(parentTaskContext, new HashMap<>(), subTaskContext, taskInfo.getTask().getOutputMappings());
                    saveContext(executionId, parentTaskContext, Sets.newHashSet(taskInfo));
                }

                executionResult.setContext(parentTaskContext);
                dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            } else if (groupStatusChanged) {
                if (taskInfo.getSubGroupIndexToStatus().get(completedGroupIndex).isCompleted()) {
                    taskInfo.getSubGroupIndexToStatus().entrySet().stream()
                            .filter(entry -> entry.getValue() == TaskStatus.READY)
                            .map(Map.Entry::getKey)
                            .findFirst()
                            .ifPresent(readyToRunGroupIndex -> {
                                taskInfo.getSubGroupIndexToStatus().put(readyToRunGroupIndex, TaskStatus.RUNNING);
                                String routName = DAGWalkHelper.getInstance().buildTaskInfoRouteName(taskInfo.getName(), readyToRunGroupIndex);
                                String mockTaskName = DAGWalkHelper.getInstance().buildTaskInfoName(routName, "foreachMockName");
                                executionResult.setTaskNameNeedToTraversal(mockTaskName);
                                log.info("finishParentTask ready to execute group:{} executionId:{}, taskInfoName:{}",
                                        readyToRunGroupIndex, executionId, taskInfo.getName());
                            });
                }
                updateTaskInfoStatusWhenKeySucceed(taskInfo, taskStatus);
                dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            }
            executionResult.setTaskStatus(taskInfo.getTaskStatus());
            executionResult.setTaskInfo(taskInfo);
        });
        return executionResult;
    }

    private void updateTaskInfoStatusWhenKeySucceed(TaskInfo taskInfo, TaskStatus taskStatus) {
        if (TaskStatus.KEY_SUCCEED.equals(taskStatus)) {
            taskInfo.setTaskStatus(taskStatus);
        }
    }

    protected Map<String, Object> getSubTaskContextMap(String executionId, TaskInfo taskInfo) {
        List<Map<String, Object>> subContextList = ContextHelper.getInstance().getSubContextList(dagContextStorage, executionId, taskInfo);
        Map<String, Object> output = Maps.newConcurrentMap();
        output.put("sub_context", subContextList);
        return output;
    }

    protected void validateDAGInfo(String executionId) {
        DAGInfo dagInfo = dagInfoStorage.getBasicDAGInfo(executionId);
        if (dagInfo == null) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_EXECUTION_NOT_FOUND.getCode(), String.format("validateDAGInfo dag executionId:%s not found", executionId));
        }
        if (dagInfo.getDagStatus().isCompleted()) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("validateDAGInfo dag executionId:%s is finished", executionId));
        }
    }

    protected void finishActionValidateTaskInfo(String executionId, String taskInfoName, TaskInfo taskInfo) {
        if (taskInfo == null) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("dag %s can not get task %s", executionId, taskInfoName));
        }
        if (taskInfo.getTaskStatus() == TaskStatus.NOT_STARTED || taskInfo.getTaskStatus() == TaskStatus.READY) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("attempt finish wrong task %s", taskInfoName));
        }
        if (taskInfo.getTaskStatus().isCompleted()) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("repeated finish task %s", taskInfoName));
        }
    }

    @SuppressWarnings("unchecked")
    protected boolean conditionsAllMatch(List<String> conditions, Map<String, Object> valueMap, String mapType) {
        return conditions.stream()
                .map(condition -> JsonPath.using(valuePathConf).parse(ImmutableMap.of(mapType, valueMap)).read(condition))
                .allMatch(it -> CollectionUtils.isNotEmpty((List<Object>) it));
    }

    @SuppressWarnings("unchecked")
    protected boolean conditionsAnyMatch(List<String> conditions, Map<String, Object> valueMap, String mapType) {
        return conditions.stream()
                .map(condition -> JsonPath.using(valuePathConf).parse(ImmutableMap.of(mapType, valueMap)).read(condition))
                .anyMatch(it -> CollectionUtils.isNotEmpty((List<Object>) it));
    }

    protected void skipFollowingTasks(String executionId, TaskInfo taskInfo, Set<TaskInfo> skippedTasks) {
        Map<String, TaskInfo> siblingTaskInfos = getSiblingTaskInfoMap(executionId, taskInfo);
        List<TaskInfo> currentTaskNext = Optional.ofNullable(siblingTaskInfos.get(taskInfo.getName())).map(TaskInfo::getNext).orElse(null);
        setNextTaskSkipStatus(siblingTaskInfos.size(), skippedTasks, currentTaskNext, Sets.newHashSet(taskInfo.getName()));
    }

    private Map<String, TaskInfo> getSiblingTaskInfoMap(String executionId, TaskInfo taskInfo) {
        Map<String, TaskInfo> siblingTaskInfos = Maps.newHashMap();
        if (DAGWalkHelper.getInstance().isAncestorTask(taskInfo.getName())) {
            Optional.ofNullable(dagInfoStorage.getBasicDAGInfo(executionId))
                    .map(DAGInfo::getTasks)
                    .ifPresent(siblingTaskInfos::putAll);
        } else {
            Optional.ofNullable(dagInfoStorage.getParentTaskInfoWithSibling(executionId, taskInfo.getName()))
                    .map(TaskInfo::getChildren).ifPresent(siblingTaskInfos::putAll);
        }
        return siblingTaskInfos;
    }

    private void setNextTaskSkipStatus(int length, Set<TaskInfo> skippedTasks, List<TaskInfo> nextTaskInfos, Set<String> dependedTaskNames) {
        if (length < 0 || CollectionUtils.isEmpty(nextTaskInfos)) {
            return;
        }

        List<TaskInfo> taskInfosNext = Lists.newArrayList();
        nextTaskInfos.forEach(taskInfo -> {
            boolean taskNeedSkip = Optional.ofNullable(taskInfo.getDependencies())
                    .filter(dependedTasks -> dependedTasks.stream()
                            .filter(Objects::nonNull)
                            .allMatch(dependedTask -> dependedTaskNames.contains(dependedTask.getName())))
                    .isPresent();
            if (taskNeedSkip) {
                taskInfo.setTaskStatus(TaskStatus.SKIPPED);
                taskInfo.updateInvokeMsg(TaskInvokeMsg.builder().msg(NORMAL_SKIP_MSG).build());
                skippedTasks.add(taskInfo);
                dependedTaskNames.add(taskInfo.getName());
                taskInfosNext.addAll(taskInfo.getNext());
            }
        });

        setNextTaskSkipStatus(length - 1, skippedTasks, taskInfosNext, dependedTaskNames);
    }
}
