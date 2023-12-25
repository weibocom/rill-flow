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

import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.concurrent.ExecutionRunnable;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.model.DAGSettings;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGResult;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.result.DAGResultHandler;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInteraction;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.helper.PluginHelper;
import com.weibo.rill.flow.olympicene.traversal.notify.NotifyType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Slf4j
public class Olympicene implements DAGInteraction {
    private final DAGInfoStorage dagInfoStorage;
    private final DAGOperations dagOperations;
    private final ExecutorService notifyExecutor;
    private final DAGResultHandler dagResultHandler;
    @Setter
    private long dagResultGetTimeoutInMillisecond = 5000;

    public Olympicene(DAGInfoStorage dagInfoStorage, DAGOperations dagOperations, ExecutorService notifyExecutor, DAGResultHandler dagResultHandler) {
        this.dagInfoStorage = dagInfoStorage;
        this.dagOperations = dagOperations;
        this.notifyExecutor = notifyExecutor;
        this.dagResultHandler = dagResultHandler;
    }

    /**
     * 提交需执行的DAG任务
     */
    public void submit(String executionId, DAG dag, Map<String, Object> data) {
        submit(executionId, dag, data, DAGSettings.DEFAULT, null);
    }

    /**
     * 提交需执行的DAG任务
     */
    @Override
    public void submit(String executionId, DAG dag, Map<String, Object> data, DAGSettings settings, NotifyInfo notifyInfo) {
        runNotify(executionId, NotifyType.SUBMIT, notifyInfo,
                () -> dagOperations.submitDAG(executionId, dag, settings, data, notifyInfo));
    }

    public void runNotify(String executionId, NotifyType notifyType, NotifyInfo notifyInfo, Runnable actions) {
        notifyExecutor.execute(new ExecutionRunnable(executionId, () -> doRunNotify(executionId, notifyType, notifyInfo, actions)));
    }

    public void doRunNotify(String executionId, NotifyType notifyType, NotifyInfo notifyInfo, Runnable actions) {
        try {
            log.info("runNotify start executionId:{}, notifyType:{}, notifyInfo:{}", executionId, notifyType, notifyInfo);

            Map<String, Object> params = Maps.newHashMap();
            params.put("executionId", executionId);
            params.put("notifyType", notifyType);
            params.put("notifyInfo", notifyInfo);

            Runnable runnable = PluginHelper.pluginInvokeChain(actions, params, SystemConfig.NOTIFY_CUSTOMIZED_PLUGINS);
            runnable.run();
        } catch (Exception e) {
            log.error("runNotify fails, executionId:{}, notifyType:{}, notifyInfo:{}", executionId, notifyType, notifyInfo, e);
            throw e;
        }
    }

    /**
     * 完成task后调用接口
     */
    @Override
    public void finish(String executionId, DAGSettings settings, Map<String, Object> data, NotifyInfo notifyInfo) {
        if (notifyInfo == null || StringUtils.isEmpty(notifyInfo.getTaskInfoName())) {
            log.warn("finish can not get dag taskName executionId:{}", executionId);
            throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "finish taskName can not be null");
        }

        runNotify(executionId, NotifyType.FINISH, notifyInfo,
                () -> dagOperations.finishTaskSync(executionId, "function", notifyInfo, data));
    }

    /**
     * 唤醒suspense任务
     */
    @Override
    public void wakeup(String executionId, Map<String, Object> data, NotifyInfo notifyInfo) {
        runNotify(executionId, NotifyType.WAKEUP, notifyInfo,
                () -> {
                    Optional.ofNullable(notifyInfo.getTaskInfoName())
                            .filter(StringUtils::isNotEmpty)
                            .ifPresent(taskInfoName -> dagOperations.finishTaskSync(executionId, TaskCategory.SUSPENSE.getValue(), notifyInfo, data));

                    Optional.ofNullable(notifyInfo.getParentTaskInfoName())
                            .filter(StringUtils::isNotEmpty)
                            .filter(parentTaskName -> !parentTaskName.equals(notifyInfo.getTaskInfoName()))
                            .ifPresent(parentTaskName -> wakeupSubTasks(executionId, data, parentTaskName));
                });
    }

    private void wakeupSubTasks(String executionId, Map<String, Object> data, String parentTaskInfoName) {
        TaskInfo taskInfo = dagInfoStorage.getTaskInfo(executionId, parentTaskInfoName);
        if (taskInfo.getTaskStatus().isCompleted()) {
            log.info("wakeupSubTasks parent task is completed, executionId:{}, parentTaskInfoName:{}", executionId, parentTaskInfoName);
            return;
        }

        Set<String> needWakeupTaskInfoNames = taskInfo.getChildren().values().stream()
                .filter(Objects::nonNull)
                .filter(subTaskInfo -> Objects.equals(subTaskInfo.getTask().getCategory(), TaskCategory.SUSPENSE.getValue()))
                .filter(subTaskInfo -> !subTaskInfo.getTaskStatus().isCompleted())
                .map(TaskInfo::getName)
                .collect(Collectors.toSet());
        needWakeupTaskInfoNames.forEach(taskInfoName ->
                dagOperations.finishTaskSync(executionId, TaskCategory.SUSPENSE.getValue(), NotifyInfo.builder().taskInfoName(taskInfoName).build(), data));
    }

    @Override
    public void redo(String executionId, Map<String, Object> data, NotifyInfo notifyInfo) {
        if (StringUtils.isEmpty(executionId)) {
            log.warn("redo executionId empty");
            throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "redo executionId can not be empty");
        }

        List<String> taskInfoNames = Optional.ofNullable(notifyInfo).map(NotifyInfo::getTaskInfoNames).orElse(Collections.emptyList());
        runNotify(executionId, NotifyType.REDO, notifyInfo, () -> dagOperations.redoTask(executionId, taskInfoNames, data));
    }

    /**
     * 执行的DAG任务并返回执行结果
     */
    public DAGResult run(String executionId, DAG dag, Map<String, Object> data) {
        return run(executionId, dag, data, DAGSettings.DEFAULT, null);
    }

    /**
     * 执行的DAG任务并返回执行结果
     */
    @Override
    public DAGResult run(String executionId, DAG dag, Map<String, Object> data, DAGSettings settings, NotifyInfo notifyInfo) {
        return run(executionId, dag, data, settings, notifyInfo, dagResultGetTimeoutInMillisecond);
    }

    /**
     * 执行的DAG任务并返回执行结果
     */
    public DAGResult run(String executionId, DAG dag, Map<String, Object> data, long timeoutInMillisecond) {
        return run(executionId, dag, data, DAGSettings.DEFAULT, null, timeoutInMillisecond);
    }

    /**
     * 执行的DAG任务并返回执行结果
     */
    public DAGResult run(String executionId, DAG dag, Map<String, Object> data, DAGSettings settings, NotifyInfo notifyInfo, long timeoutInMillisecond) {
        if (dagResultHandler == null) {
            log.warn("run nonsupport due to dagResultHandler is null executionId:{}", executionId);
            throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "run nonsupport due to dagResultHandler is null");
        }

        dagResultHandler.initEnv(executionId);
        doRunNotify(executionId, NotifyType.RUN, notifyInfo,
                () -> dagOperations.submitDAG(executionId, dag, settings, data, notifyInfo));
        return dagResultHandler.getDAGResult(executionId, timeoutInMillisecond);
    }
}
