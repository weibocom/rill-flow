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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.strategy.Retry;
import com.weibo.rill.flow.olympicene.core.model.strategy.RetryContext;
import com.weibo.rill.flow.olympicene.core.model.strategy.RetryPolicy;
import com.weibo.rill.flow.olympicene.core.model.strategy.SimpleRetryPolicy;
import com.weibo.rill.flow.interfaces.model.task.FunctionPattern;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import com.weibo.rill.flow.interfaces.model.task.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.weibo.rill.flow.interfaces.model.task.FunctionPattern.*;

/**
 * 方法加锁原因
 * 任务执行速度较快 doRun通知执行者后 尚未完成任务状态存储 执行者完成任务执行并调finish 从而产生并发
 */
@Slf4j
public class FunctionTaskRunner extends AbstractTaskRunner {
    private final DAGDispatcher dagDispatcher;
    private final RetryPolicy retryPolicy;

    public FunctionTaskRunner(DAGDispatcher dagDispatcher,
                              InputOutputMapping inputOutputMapping,
                              DAGContextStorage dagContextStorage,
                              DAGInfoStorage dagInfoStorage,
                              DAGStorageProcedure dagStorageProcedure,
                              SwitcherManager switcherManager) {
        this(dagDispatcher, inputOutputMapping, dagContextStorage, dagInfoStorage, dagStorageProcedure,
                new SimpleRetryPolicy(), switcherManager);
    }

    public FunctionTaskRunner(DAGDispatcher dagDispatcher,
                              InputOutputMapping inputOutputMapping,
                              DAGContextStorage dagContextStorage,
                              DAGInfoStorage dagInfoStorage,
                              DAGStorageProcedure dagStorageProcedure,
                              RetryPolicy retryPolicy,
                              SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
        this.dagDispatcher = dagDispatcher;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.FUNCTION;
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("function task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        AtomicReference<ExecutionResult> executionRef = new AtomicReference<>();
        dagStorageProcedure.lockAndRun(LockerKey.buildTaskInfoLockName(executionId, taskInfo.getName()), () -> {
            FunctionPattern functionPattern = ((FunctionTask) taskInfo.getTask()).getPattern();
            switch (functionPattern) {
                case TASK_SYNC:
                case TASK_SCHEDULER:
                case TASK_ASYNC:
                    executionRef.set(dispatchTask(executionId, taskInfo, input, functionPattern, TaskStatus::isSuccessOrSkip));
                    break;
                case FLOW_SYNC:
                case FLOW_ASYNC:
                    executionRef.set(dispatchTask(executionId, taskInfo, input, functionPattern, t -> !t.isFailed()));
                    break;
                default:
                    throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), String.format("%s not supported", functionPattern));
            }
        });

        log.info("run function task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        return executionRef.get();
    }

    private ExecutionResult dispatchTask(String executionId, TaskInfo taskInfo, Map<String, Object> input,
                                         FunctionPattern functionPattern,
                                         Function<TaskStatus, Boolean> needUpdateContext) {
        DispatchInfo dispatchInfo = DispatchInfo.builder()
                .taskInfo(taskInfo)
                .input(input)
                .executionId(executionId)
                .build();

        try {
            String dispatchRet = dagDispatcher.dispatch(dispatchInfo);
            JsonNode dispatchRetJson = getRetJson(dispatchRet);

            TaskInvokeMsg taskInvokeMsg = buildInvokeMsg(dispatchRetJson);
            Map<String, Object> output = buildOutput(dispatchRetJson);
            NotifyInfo notifyInfo = NotifyInfo.builder()
                    .taskInfoName(taskInfo.getName())
                    .taskStatus(buildTaskStatus(output, taskInfo, taskInvokeMsg))
                    .taskInvokeMsg(taskInvokeMsg)
                    .build();

            return executionCallback(executionId, taskInfo, notifyInfo, output, needUpdateContext);
        } catch (Exception e) {
            log.error("dispatchTask fails, executionId:{}, taskName:{}, functionPattern:{}, errorMsg:{}",
                    executionId, taskInfo.getName(), functionPattern, e.getMessage());

            Retry retry = ((FunctionTask) taskInfo.getTask()).getRetry();
            RetryContext retryContext = RetryContext.builder().retryConfig(retry).taskStatus(TaskStatus.FAILED).taskInfo(taskInfo).build();
            if (retryPolicy.needRetry(retryContext)) {
                NotifyInfo notifyInfo = NotifyInfo.builder().retryContext(retryContext).build();
                if (Optional.ofNullable(taskInfo.getTaskInvokeMsg()).map(TaskInvokeMsg::getMsg).isEmpty()) {
                    notifyInfo.setTaskInvokeMsg(TaskInvokeMsg.builder().msg(e.getMessage()).build());
                }
                return handleRetryCallback(executionId, taskInfo, notifyInfo);
            }

            throw new DAGTraversalException(TraversalErrorCode.TRAVERSAL_FAILED.getCode(), e.getMessage());
        }
    }

    private Map<String, Object> buildOutput(JsonNode dispatchRetJson) {
        if (dispatchRetJson == null) {
            return null;
        }

        Map<String, Object> output = Maps.newHashMap();
        if (dispatchRetJson.isObject()) {
            output.putAll(DAGTraversalSerializer.MAPPER.convertValue(dispatchRetJson, new TypeReference<Map<String, Object>>() {
            }));
        } else if (dispatchRetJson.isArray()) {
            output.put("data", DAGTraversalSerializer.MAPPER.convertValue(dispatchRetJson, new TypeReference<List<Object>>() {
            }));
        }
        return output;
    }

    private TaskStatus buildTaskStatus(Map<String, Object> output, TaskInfo taskInfo, TaskInvokeMsg taskInvokeMsg) {
        FunctionTask functionTask = (FunctionTask) taskInfo.getTask();
        FunctionPattern functionPattern = functionTask.getPattern();

        if (functionPattern == TASK_SCHEDULER || functionPattern == TASK_ASYNC) {
            return TaskStatus.RUNNING;
        } else if (functionPattern == TASK_SYNC) {
            return taskTypeStatus(output, functionTask, null);
        } else if (functionPattern == FLOW_ASYNC || functionPattern == FLOW_SYNC) {
            return flowTypeStatus(taskInvokeMsg, functionPattern);
        }
        return TaskStatus.RUNNING;
    }

    private TaskStatus taskTypeStatus(Map<String, Object> output, FunctionTask functionTask, TaskStatus taskStatus) {
        if (output == null) {
            log.warn("output is null");
            return TaskStatus.FAILED;
        }
        if (CollectionUtils.isNotEmpty(functionTask.getSuccessConditions())) {
            return conditionsAllMatch(functionTask.getSuccessConditions(), output, "output") ? TaskStatus.SUCCEED : TaskStatus.FAILED;
        }
        if (CollectionUtils.isNotEmpty(functionTask.getFailConditions())) {
            return conditionsAllMatch(functionTask.getFailConditions(), output, "output") ? TaskStatus.FAILED : TaskStatus.SUCCEED;
        }
        if (taskStatus != null) {
            return taskStatus;
        }
        return Optional.of(output)
                .map(it -> it.get("result_type"))
                .map(String::valueOf)
                .filter(resultType -> !"SUCCESS".equalsIgnoreCase(resultType))
                .map(type -> TaskStatus.FAILED)
                .orElse(TaskStatus.SUCCEED);
    }

    private TaskStatus flowTypeStatus(TaskInvokeMsg taskInvokeMsg, FunctionPattern functionPattern) {
        return Optional.ofNullable(taskInvokeMsg)
                .map(TaskInvokeMsg::getReferencedDAGExecutionId)
                .filter(StringUtils::isNotBlank)
                .map(id -> functionPattern == FLOW_ASYNC ? TaskStatus.SUCCEED : TaskStatus.RUNNING)
                .orElse(TaskStatus.FAILED);
    }

    private TaskInvokeMsg buildInvokeMsg(JsonNode dispatchRetJson) {
        TaskInvokeMsg taskInvokeMsg = new TaskInvokeMsg();
        try {
            if (dispatchRetJson == null || !dispatchRetJson.isObject()) {
                return taskInvokeMsg;
            }
            appendInvokeInfo(dispatchRetJson, taskInvokeMsg);

            JsonNode data = dispatchRetJson.get("data");
            if (data != null && data.isObject()) {
                appendInvokeInfo(data, taskInvokeMsg);
            }
        } catch (Exception e) {
            // not standard ret value format ignore
        }
        return taskInvokeMsg;
    }

    private void appendInvokeInfo(JsonNode ret, TaskInvokeMsg taskInvokeMsg) {
        JsonNode code = ret.has("error_code") ? ret.get("error_code") : ret.get("code");
        Optional.ofNullable(code).map(JsonNode::asText).ifPresent(taskInvokeMsg::setCode);

        JsonNode msg = ret.has("error_msg") ? ret.get("error_msg") : ret.get("msg");
        Optional.ofNullable(msg).map(JsonNode::asText).ifPresent(taskInvokeMsg::setMsg);

        JsonNode invokeId = ret.get("invoke_id");
        Optional.ofNullable(invokeId).map(JsonNode::asText).ifPresent(taskInvokeMsg::setInvokeId);

        JsonNode executionId = ret.get("execution_id");
        Optional.ofNullable(executionId).map(JsonNode::asText).ifPresent(taskInvokeMsg::setReferencedDAGExecutionId);

        if (switcherManager.getSwitcherState("ENABLE_SET_INPUT_OUTPUT")) {
            Map<String, Object> retMap = new ObjectMapper().convertValue(ret, new TypeReference<>() {
            });
            taskInvokeMsg.setOutput(retMap);
        }
    }

    private JsonNode getRetJson(String dispatchRet) {
        try {
            return StringUtils.isBlank(dispatchRet) ? null : DAGTraversalSerializer.MAPPER.readTree(dispatchRet);
        } catch (Exception e) {
            log.warn("getRetJson fails, dispatchRet:{}, errorMsg:{}", dispatchRet, e.getMessage());
            return null;
        }
    }

    @Override
    public ExecutionResult finish(String executionId, NotifyInfo notifyInfo, Map<String, Object> output) {
        log.info("function task begin to finish executionId:{}, notifyInfo:{}", executionId, notifyInfo);
        AtomicReference<ExecutionResult> executionRef = new AtomicReference<>();
        dagStorageProcedure.lockAndRun(LockerKey.buildTaskInfoLockName(executionId, notifyInfo.getTaskInfoName()), () -> {
            validateDAGInfo(executionId);

            TaskInfo taskInfo = dagInfoStorage.getBasicTaskInfo(executionId, notifyInfo.getTaskInfoName());
            finishActionValidateTaskInfo(executionId, notifyInfo.getTaskInfoName(), taskInfo);

            FunctionTask functionTask = (FunctionTask) taskInfo.getTask();
            notifyInfo.setTaskStatus(taskTypeStatus(output, functionTask, notifyInfo.getTaskStatus()));
            Function<TaskStatus, Boolean> needUpdateContext = t -> !t.isFailed()
                    || CollectionUtils.isNotEmpty(functionTask.getSuccessConditions())
                    || CollectionUtils.isNotEmpty(functionTask.getFailConditions());
            executionRef.set(executionCallback(executionId, taskInfo, notifyInfo, output, needUpdateContext));
        });

        return executionRef.get();
    }

    private ExecutionResult executionCallback(String executionId, TaskInfo taskInfo, NotifyInfo notifyInfo,
                                              Map<String, Object> output, Function<TaskStatus, Boolean> needUpdateContext) {
        RetryContext retryContext = RetryContext.builder().retryConfig(((FunctionTask) taskInfo.getTask()).getRetry())
                .taskStatus(notifyInfo.getTaskStatus()).taskInfo(taskInfo).build();
        boolean needRetry = retryPolicy.needRetry(retryContext);
        log.info("executionCallback start executionId:{}, taskInfoName:{}, needRetry:{}", executionId, taskInfo.getName(), needRetry);

        if (needRetry) {
            notifyInfo.setRetryContext(retryContext);
            return handleRetryCallback(executionId, taskInfo, notifyInfo);
        }

        return handleNormalCallback(executionId, taskInfo, notifyInfo, output, needUpdateContext);
    }

    private ExecutionResult handleNormalCallback(String executionId, TaskInfo taskInfo, NotifyInfo notifyInfo,
                                                 Map<String, Object> output, Function<TaskStatus, Boolean> needUpdateContext) {
        log.info("handleNormalCallback executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        TaskStatus notifyTaskStatus = notifyInfo.getTaskStatus();

        taskInfo.updateInvokeMsg(notifyInfo.getTaskInvokeMsg());
        taskInfo.setTaskStatus(notifyTaskStatus == TaskStatus.FAILED && taskInfo.getTask().isTolerance() ? TaskStatus.SKIPPED : notifyTaskStatus);
        if (taskInfo.getTaskStatus().isCompleted()) {
            updateTaskInvokeEndTime(taskInfo);
        }

        // context需要在taskInfo之前更新
        // 当taskInfo先更新 context更新未完成时，有其他分支的任务完成，触发任务遍历，此时后续任务会开始执行，但context未更新完成，导致后续任务数据无法获取
        Map<String, Object> context = null;
        List<Mapping> outputMappings = taskInfo.getTask().getOutputMappings();
        if (needUpdateContext.apply(taskInfo.getTaskStatus()) && MapUtils.isNotEmpty(output) && CollectionUtils.isNotEmpty(outputMappings)) {
            context = ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);
            outputMappings(context, new HashMap<>(), output, outputMappings);
            saveContext(executionId, context, Sets.newHashSet(taskInfo));
        }

        dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));

        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).taskInfo(taskInfo).context(context).build();
    }

    private ExecutionResult handleRetryCallback(String executionId, TaskInfo taskInfo, NotifyInfo notifyInfo) {
        log.info("handleRetryCallback executionId:{} taskInfoName:{}", executionId, taskInfo.getName());

        taskInfo.setTaskStatus(TaskStatus.READY);
        taskInfo.updateInvokeMsg(notifyInfo.getTaskInvokeMsg());
        updateTaskInvokeEndTime(taskInfo);

        dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));

        int retryInterval = retryPolicy.calculateRetryInterval(notifyInfo.getRetryContext());

        // retryInterval == 0 表示立即开始重试 任务执行需要context信息
        Map<String, Object> context = retryInterval != 0 ?
                null : ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);

        return ExecutionResult.builder()
                .needRetry(true).retryIntervalInSeconds(retryInterval)
                .taskStatus(taskInfo.getTaskStatus()).taskInfo(taskInfo)
                .context(context)
                .build();
    }
}
