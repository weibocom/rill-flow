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

package com.weibo.rill.flow.olympicene.traversal;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.strategy.Timeline;
import com.weibo.rill.flow.interfaces.model.task.*;
import com.weibo.rill.flow.olympicene.core.concurrent.ExecutionRunnable;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.event.Callback;
import com.weibo.rill.flow.olympicene.core.model.DAGSettings;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.*;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg.ExecutionInfo;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.result.DAGResultHandler;
import com.weibo.rill.flow.olympicene.traversal.callback.CallbackInvoker;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.helper.PluginHelper;
import com.weibo.rill.flow.olympicene.traversal.runners.DAGRunner;
import com.weibo.rill.flow.olympicene.traversal.runners.TaskRunner;
import com.weibo.rill.flow.olympicene.traversal.runners.TimeCheckRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

@Slf4j
public class DAGOperations {
    private static final String EXECUTION_ID = "executionId";

    private final ExecutorService runnerExecutor;
    private final Map<String, TaskRunner> taskRunners;
    private final DAGRunner dagRunner;
    private final TimeCheckRunner timeCheckRunner;
    private final DAGTraversal dagTraversal;
    private final Callback<DAGCallbackInfo> callback;
    private final DAGResultHandler dagResultHandler;


    public static final BiConsumer<Runnable, Integer> OPERATE_WITH_RETRY = (operation, retryTimes) -> {
        int exceptionCatchTimes = retryTimes;
        for (int i = 1; i <= exceptionCatchTimes; i++) {
            try {
                operation.run();
                return;
            } catch (Exception e) {
                log.warn("operateWithRetry fails, invokeTimes:{}", i, e);
            }
        }

        operation.run();
    };

    public DAGOperations(ExecutorService runnerExecutor, Map<String, TaskRunner> taskRunners, DAGRunner dagRunner,
                         TimeCheckRunner timeCheckRunner, DAGTraversal dagTraversal, Callback<DAGCallbackInfo> callback,
                         DAGResultHandler dagResultHandler) {
        this.runnerExecutor = runnerExecutor;
        this.taskRunners = taskRunners;
        this.dagRunner = dagRunner;
        this.timeCheckRunner = timeCheckRunner;
        this.dagTraversal = dagTraversal;
        this.callback = callback;
        this.dagResultHandler = dagResultHandler;
    }

    public void runTasks(String executionId, Collection<Pair<TaskInfo, Map<String, Object>>> taskInfoToContexts) {
        log.info("runTasks begin submit task executionId:{}", executionId);
        taskInfoToContexts.forEach(taskInfoToContext -> runnerExecutor.execute(new ExecutionRunnable(executionId, () -> {
            TaskInfo taskInfo = taskInfoToContext.getLeft();
            try {
                log.info("runTasks task begin to execute executionId:{} taskInfoName:{}", executionId, taskInfo.getName());
                runTask(executionId, taskInfo, taskInfoToContext.getRight());
            } catch (Exception e) {
                log.error("runTasks fails, executionId:{}, taskName:{}", executionId, taskInfo.getName(), e);
            }
        })));
    }


    private void runTask(String executionId, TaskInfo taskInfo, Map<String, Object> context) {
        Map<String, Object> params = Maps.newHashMap();
        params.put(EXECUTION_ID, executionId);
        params.put("taskInfo", taskInfo);
        params.put("context", context);

        TaskRunner runner = selectRunner(taskInfo);
        Supplier<ExecutionResult> basicActions = () -> runner.run(executionId, taskInfo, context);

        Supplier<ExecutionResult> supplier = PluginHelper.pluginInvokeChain(basicActions, params, SystemConfig.TASK_RUN_CUSTOMIZED_PLUGINS);
        ExecutionResult executionResult = supplier.get();

        /*
          任务执行后结果类型
          1. 任务执行完成 如 return/pass
            1.1 任务执行完成 需要寻找下一个能执行的任务
            1.2 任务执行中 需要外部系统调finish触发下一个任务执行
            1.3 任务需要重试
          2. 流程控制类任务 foreach/choice
            2.1 触发能够执行的子任务
         */
        // 对应1.1
        if (isTaskCompleted(executionResult)) {
            dagTraversal.submitTraversal(executionId, taskInfo.getName());
            invokeTaskCallback(executionId, taskInfo, context);
        }
        // 对应1.2
        if (executionResult.getTaskStatus() == TaskStatus.RUNNING) {
            Timeline timeline = Optional.ofNullable(taskInfo.getTask()).map(BaseTask::getTimeline).orElse(null);
            Optional.ofNullable(getTimeoutSeconds(executionResult.getInput(), new HashMap<>(), timeline))
                    .ifPresent(timeoutSeconds -> timeCheckRunner.addTaskToTimeoutCheck(executionId, taskInfo, timeoutSeconds));
        }
        // 对应1.3
        if (executionResult.getTaskStatus() == TaskStatus.READY && executionResult.isNeedRetry()) {
            runTaskWithTimeInterval(executionId, executionResult.getTaskInfo(),
                    executionResult.getContext(), executionResult.getRetryIntervalInSeconds());
        }
        // 对应2.1
        if (CollectionUtils.isNotEmpty(executionResult.getSubTaskInfosAndContext())) {
            executionResult.getSubTaskInfosAndContext()
                    .forEach(subTaskInfosAndContext -> {
                        dagTraversal.submitTasks(executionId, subTaskInfosAndContext.getLeft(), subTaskInfosAndContext.getRight());
                        safeSleep(10);
                    });
        }
    }

    private Long getTimeoutSeconds(Map<String, Object> input, Map<String, Object> context, Timeline timeline) {
        try {
            if (timeline == null || StringUtils.isBlank(timeline.getTimeoutInSeconds())) {
                return null;
            }

            List<Mapping> timeMappings = Lists.newArrayList();
            timeMappings.add(new Mapping(timeline.getTimeoutInSeconds(), "$.output.timeout"));
            Map<String, Object> output = Maps.newHashMap();
            taskRunners.get("function").inputMappings(context, input, output, timeMappings);
            return Optional.ofNullable(output.get("timeout"))
                    .map(String::valueOf)
                    .map(Long::valueOf)
                    .filter(it -> it > 0L)
                    .orElse(null);
        } catch (Exception e) {
            log.warn("can not get timeout in seconds, source:{} errorMsg:{}", timeline, e.getMessage());
            return null;
        }
    }

    private void runTaskWithTimeInterval(String executionId, TaskInfo taskInfo, Map<String, Object> context, int intervalInSeconds) {
        log.info("runTaskWithTimeInterval task start to check executionId:{} taskInfoName:{} intervalInSeconds:{}",
                executionId, taskInfo.getName(), intervalInSeconds);
        if (intervalInSeconds > 0) {
            timeCheckRunner.addTaskToWaitCheck(executionId, taskInfo, intervalInSeconds);
            return;
        }
        runTasks(executionId, Lists.newArrayList(Pair.of(taskInfo, context)));
    }

    public void finishTaskAsync(String executionId, String taskCategory, NotifyInfo notifyInfo, Map<String, Object> output) {
        runnerExecutor.execute(new ExecutionRunnable(executionId, () -> {
            try {
                finishTaskSync(executionId, taskCategory, notifyInfo, output);
            } catch (Exception e) {
                log.error("finishTaskAsync fails, executionId:{}, taskCategory:{}, notifyInfo:{}",
                        executionId, taskCategory, notifyInfo, e);
            }
        }));
    }

    public void finishTaskSync(String executionId, String taskCategory, NotifyInfo notifyInfo, Map<String, Object> output) {
        log.info("finishTask task begin to execute executionId:{} notifyInfo:{}", executionId, notifyInfo);
        Map<String, Object> params = Maps.newHashMap();
        params.put(EXECUTION_ID, executionId);
        params.put("taskCategory", taskCategory);
        params.put("notifyInfo", notifyInfo);
        params.put("output", output);
        TaskRunner runner = selectRunner(taskCategory);
        Supplier<ExecutionResult> basicActions = () -> runner.finish(executionId, notifyInfo, output);
        Supplier<ExecutionResult> supplier = PluginHelper.pluginInvokeChain(basicActions, params, SystemConfig.TASK_FINISH_CUSTOMIZED_PLUGINS);
        ExecutionResult executionResult = supplier.get();

        if (executionResult.getTaskStatus() == TaskStatus.READY && executionResult.isNeedRetry()) {
            timeCheckRunner.remTaskFromTimeoutCheck(executionId, executionResult.getTaskInfo());
            runTaskWithTimeInterval(executionId, executionResult.getTaskInfo(),
                    executionResult.getContext(), executionResult.getRetryIntervalInSeconds());
        }
        if (isTaskCompleted(executionResult)) {
            timeCheckRunner.remTaskFromTimeoutCheck(executionId, executionResult.getTaskInfo());
            dagTraversal.submitTraversal(executionId, executionResult.getTaskInfo().getName());
            invokeTaskCallback(executionId, executionResult.getTaskInfo(), executionResult.getContext());
        }
        if (StringUtils.isNotBlank(executionResult.getTaskNameNeedToTraversal())) {
            dagTraversal.submitTraversal(executionId, executionResult.getTaskNameNeedToTraversal());
        }

        // key finished
        if (isForeachTaskKeyCompleted(executionResult, notifyInfo.getCompletedGroupIndex())
                || isSubFlowTaskKeyCompleted(executionResult)) {
            dagTraversal.submitTraversal(executionId, executionResult.getTaskInfo().getName());
        }
    }

    public void redoTask(String executionId, List<String> taskNames, Map<String, Object> data) {
        log.info("redoTask task begin to execute executionId:{} taskNames:{}", executionId, taskNames);
        dagRunner.resetTask(executionId, taskNames, data);
        dagTraversal.submitTraversal(executionId, null);
    }

    public void submitDAG(String executionId, DAG dag, DAGSettings settings, Map<String, Object> data, NotifyInfo notifyInfo) {
        log.info("submitDAG task begin to execute executionId:{} notifyInfo:{}", executionId, notifyInfo);
        ExecutionResult executionResult = dagRunner.submitDAG(executionId, dag, settings, data, notifyInfo);
        Optional.ofNullable(getTimeoutSeconds(new HashMap<>(), executionResult.getContext(), dag.getTimeline()))
                .ifPresent(timeoutSeconds -> timeCheckRunner.addDAGToTimeoutCheck(executionId, timeoutSeconds));
        dagTraversal.submitTraversal(executionId, null);
    }

    public void finishDAG(String executionId, DAGInfo dagInfo, DAGStatus dagStatus, DAGInvokeMsg dagInvokeMsg) {
        log.info("finishDAG task begin to execute executionId:{} dagStatus:{}", executionId, dagStatus);
        Map<String, Object> params = Maps.newHashMap();
        params.put(EXECUTION_ID, executionId);
        params.put("dagInfo", dagInfo);
        params.put("dagStatus", dagStatus);
        params.put("dagInvokeMsg", dagInvokeMsg);
        Supplier<ExecutionResult> basicActions = () -> dagRunner.finishDAG(executionId, dagInfo, dagStatus, dagInvokeMsg);

        Supplier<ExecutionResult> supplier = PluginHelper.pluginInvokeChain(basicActions, params, SystemConfig.DAG_FINISH_CUSTOMIZED_PLUGINS);
        ExecutionResult executionResult = supplier.get();

        DAGInfo dagInfoRet = executionResult.getDagInfo();
        Map<String, Object> context = executionResult.getContext();

        timeCheckRunner.remDAGFromTimeoutCheck(executionId, dagInfoRet.getDag());

        List<ExecutionInfo> executionRoutes = Optional.ofNullable(dagInfoRet.getDagInvokeMsg())
                .map(DAGInvokeMsg::getExecutionRoutes)
                .orElse(new ArrayList<>());
        executionRoutes.stream()
                .max(Comparator.comparingInt(ExecutionInfo::getIndex))
                .filter(executionInfo -> executionInfo.getExecutionType() == FunctionPattern.FLOW_SYNC)
                .ifPresent(executionInfo -> {
                    try {
                        String parentDAGExecutionId = executionInfo.getExecutionId();
                        String taskInfoName = executionInfo.getTaskInfoName();
                        TaskStatus taskStatus = calculateSubFlowTaskStatus(dagInfoRet);
                        TaskInvokeMsg taskInvokeMsg = Optional.ofNullable(dagInfoRet.getDagInvokeMsg())
                                .map(it -> TaskInvokeMsg.builder().code(it.getCode()).msg(it.getMsg()).ext(it.getExt()).build())
                                .orElse(null);
                        NotifyInfo notifyInfo = NotifyInfo.builder().taskInfoName(taskInfoName).taskStatus(taskStatus).taskInvokeMsg(taskInvokeMsg).build();
                        finishTaskSync(parentDAGExecutionId, TaskCategory.FUNCTION.getValue(), notifyInfo, context);
                    } catch (Exception e) {
                        log.warn("finishDAG fails to finish task, executionInfo:{}", executionInfo, e);
                    }
                });

        trialClose(executionId, dagStatus, dagInfoRet, context);
    }

    private TaskStatus calculateSubFlowTaskStatus(DAGInfo dagInfoRet) {
        DAGStatus dagStatus = dagInfoRet.getDagStatus();
        if (dagStatus == DAGStatus.SUCCEED) {
            return TaskStatus.SUCCEED;
        } else if (dagStatus == DAGStatus.KEY_SUCCEED) {
            return TaskStatus.KEY_SUCCEED;
        } else {
            return TaskStatus.FAILED;
        }
    }

    private void trialClose(String executionId, DAGStatus dagStatus, DAGInfo dagInfoRet, Map<String, Object> context) {
        DAGEvent dagEvent;
        if (dagStatus == DAGStatus.SUCCEED) {
            dagEvent = DAGEvent.DAG_SUCCEED;
        } else if (dagStatus == DAGStatus.KEY_SUCCEED) {
            dagEvent = DAGEvent.DAG_KEY_SUCCEED;
        } else {
            dagEvent = DAGEvent.DAG_FAILED;
        }
        invokeCallback(executionId, dagEvent, dagInfoRet, null, context);
        setDAGResult(executionId, dagInfoRet, context);
    }

    private void setDAGResult(String executionId, DAGInfo dagInfo, Map<String, Object> context) {
        log.info("setDAGResult operations executionId:{}", executionId);
        if (dagResultHandler == null) {
            log.info("setDAGResult dagResultHandler null");
            return;
        }
        dagResultHandler.updateDAGResult(executionId, DAGResult.builder().dagInfo(dagInfo).context(context).build());
    }

    private void invokeTaskCallback(String executionId, TaskInfo taskInfo, Map<String, Object> context) {
        DAGEvent dagEvent = DAGEvent.TASK_FINISH;
        if (taskInfo.getTaskStatus() == TaskStatus.FAILED) {
            dagEvent = DAGEvent.TASK_FAILED;
        } else if (taskInfo.getTaskStatus() == TaskStatus.SKIPPED) {
            dagEvent = DAGEvent.TASK_SKIPPED;
        }

        DAGInfo dagInfoMock = new DAGInfo();
        dagInfoMock.setExecutionId(executionId);

        invokeCallback(executionId, dagEvent, dagInfoMock, taskInfo, context);
    }

    private void invokeCallback(String executionId, DAGEvent dagEvent, DAGInfo dagInfo, TaskInfo taskInfo, Map<String, Object> context) {
        log.info("invokeCallback operations executionId:{} dagEvent:{} taskName:{}",
                executionId, dagEvent, Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null));
        CallbackInvoker.getInstance().callback(callback, dagEvent, dagInfo, context, taskInfo);
    }

    private boolean isTaskCompleted(ExecutionResult executionResult) {
        return Optional.ofNullable(executionResult.getTaskStatus())
                .map(TaskStatus::isCompleted)
                .orElse(false);
    }

    private boolean isForeachTaskKeyCompleted(ExecutionResult executionResult, String index) {
        return Optional.ofNullable(executionResult.getTaskStatus()).map(TaskStatus.KEY_SUCCEED::equals).orElse(false)
                && StringUtils.isNotEmpty(index)
                && Optional.ofNullable(executionResult.getTaskInfo())
                .map(TaskInfo::getSubGroupKeyJudgementMapping)
                .map(it -> it.get(index))
                .filter(Boolean.TRUE::equals)
                .orElse(false);
    }

    private boolean isSubFlowTaskKeyCompleted(ExecutionResult executionResult) {
        return Optional.ofNullable(executionResult.getTaskStatus()).map(TaskStatus.KEY_SUCCEED::equals).orElse(false)
                && Optional.ofNullable(executionResult.getTaskInfo()).map(TaskInfo::getTask)
                .filter(FunctionTask.class::isInstance)
                .map(FunctionTask.class::cast)
                .map(FunctionTask::getPattern)
                .map(it -> FunctionPattern.FLOW_SYNC.equals(it) || FunctionPattern.FLOW_ASYNC.equals(it))
                .orElse(false);
    }

    private TaskRunner selectRunner(String taskCategory) {
        TaskRunner runner = taskRunners.get(taskCategory);
        if (runner == null) {
            throw new DAGTraversalException(TraversalErrorCode.TRAVERSAL_FAILED.getCode(), "runner is null.");
        }
        return runner;
    }

    private TaskRunner selectRunner(TaskInfo taskInfo) {
        return selectRunner(taskInfo.getTask().getCategory());
    }

    private void safeSleep(long time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
            // do nothing
        }
    }
}
