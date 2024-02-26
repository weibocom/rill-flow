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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.SuspenseTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 方法加锁原因 有两个场景能触发suspense任务执行 可能产生并发
 * 1. traversal遍历触发任务
 * 2. wakeup调用触发任务
 * <p>
 * Created by xilong on 2021/4/7.
 */
@Slf4j
public class SuspenseTaskRunner extends AbstractTaskRunner {
    public SuspenseTaskRunner(InputOutputMapping inputOutputMapping,
                              DAGInfoStorage dagInfoStorage,
                              DAGContextStorage dagContextStorage,
                              DAGStorageProcedure dagStorageProcedure,
                              SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.SUSPENSE;
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("suspense task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        ExecutionResult ret = ExecutionResult.builder().build();
        dagStorageProcedure.lockAndRun(LockerKey.buildTaskInfoLockName(executionId, taskInfo.getName()), () -> {
            // 因为有并发可能 导致收到的input可能为wakeup更新前的值
            // wakeup更新可能导致context值发生变化 因此需要取最新context
            Map<String, Object> context = ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);
            Map<String, Object> inputRealTime = Maps.newHashMap();
            inputMappings(context, inputRealTime, new HashMap<>(), taskInfo.getTask().getInputMappings());

            taskInfo.setTaskStatus(TaskStatus.RUNNING);
            tryWakeup(executionId, taskInfo, inputRealTime);
            tryInterruptSuspense(executionId, taskInfo, inputRealTime);

            dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            ret.setInput(inputRealTime);
            ret.setTaskStatus(taskInfo.getTaskStatus());
        });

        log.info("run suspense task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        return ret;
    }

    @Override
    public ExecutionResult finish(String executionId, NotifyInfo notifyInfo, Map<String, Object> output) {
        String taskInfoName = notifyInfo.getTaskInfoName();
        log.info("suspense wakeup begin to run executionId:{}, taskInfoName:{}, output empty:{}",
                executionId, taskInfoName, MapUtils.isEmpty(output));

        AtomicReference<TaskInfo> taskInfoRef = new AtomicReference<>();
        AtomicReference<Map<String, Object>> contextRef = new AtomicReference<>();
        dagStorageProcedure.lockAndRun(LockerKey.buildTaskInfoLockName(executionId, taskInfoName), () -> {
            validateDAGInfo(executionId);

            TaskInfo taskInfo = dagInfoStorage.getBasicTaskInfo(executionId, taskInfoName);
            taskInfoRef.set(taskInfo);
            // 若任务状态为已完成 则不进行进一步操作
            taskInfoValid(executionId, taskInfoName, taskInfo);

            // 目前只有超时不为null
            if (notifyInfo.getTaskStatus() != null && notifyInfo.getTaskStatus().isCompleted()) {
                taskInfo.updateInvokeMsg(notifyInfo.getTaskInvokeMsg());
                updateTaskInvokeEndTime(taskInfo);
                taskInfo.setTaskStatus(notifyInfo.getTaskStatus());
                dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
                return;
            }

            // 若任务状态为非running 表示该任务尚未被调度 只更新context
            // 若任务状态为running 则更新context并尝试唤醒任务
            log.info("suspense wakeup taskInfo current taskStatus:{}", taskInfo.getTaskStatus());
            if (taskInfo.getTaskStatus() != TaskStatus.RUNNING && MapUtils.isEmpty(output)) {
                return;
            }

            Map<String, Object> context = ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);
            contextRef.set(context);
            if (MapUtils.isNotEmpty(output) && CollectionUtils.isNotEmpty(taskInfo.getTask().getOutputMappings())) {
                outputMappings(context, new HashMap<>(), output, taskInfo.getTask().getOutputMappings());
                saveContext(executionId, context, Sets.newHashSet(taskInfo));
            }

            if (taskInfo.getTaskStatus() != TaskStatus.RUNNING) {
                return;
            }
            Map<String, Object> input = Maps.newHashMap();
            inputMappings(context, input, new HashMap<>(), taskInfo.getTask().getInputMappings());
            if (tryWakeup(executionId, taskInfo, input) ||
                    tryInterruptSuspense(executionId, taskInfo, input)) {
                dagInfoStorage.saveTaskInfos(executionId, Sets.newHashSet(taskInfo));
            }
        });

        TaskInfo taskInfo = taskInfoRef.get();
        log.info("run suspense wakeup completed, executionId:{}, taskInfoName:{}, taskStatus:{}",
                executionId, taskInfoName, taskInfo.getTaskStatus());
        return ExecutionResult.builder()
                .taskStatus(taskInfo.getTaskStatus())
                .taskInfo(taskInfo)
                .context(contextRef.get())
                .build();
    }

    private boolean tryWakeup(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        if (taskInfo.getTaskStatus() != TaskStatus.RUNNING) {
            return false;
        }
        boolean needWakeup = isNeedWakeup(taskInfo, input);
        log.info("suspense task need wakeup value:{}, executionId:{}, taskInfoName:{}", needWakeup, executionId, taskInfo.getName());
        if (needWakeup) {
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
            updateTaskInvokeEndTime(taskInfo);
        }
        return needWakeup;
    }

    private boolean tryInterruptSuspense(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        if (taskInfo.getTaskStatus() != TaskStatus.RUNNING) {
            return false;
        }
        boolean needInterrupt = isNeedInterrupt(taskInfo, input);
        log.info("suspense task need interrupt value:{}, executionId:{}, taskInfoName:{}", needInterrupt, executionId, taskInfo.getName());
        if (needInterrupt) {
            taskInfo.setTaskStatus(TaskStatus.FAILED);
            taskInfo.updateInvokeMsg(TaskInvokeMsg.builder().msg("interruption").build());
            updateTaskInvokeEndTime(taskInfo);
        }
        return needInterrupt;
    }

    private boolean isNeedWakeup(TaskInfo taskInfo, Map<String, Object> input) {
        SuspenseTask suspenseTask = (SuspenseTask) taskInfo.getTask();
        return conditionsAllMatch(suspenseTask.getConditions(), input, "input");
    }

    private boolean isNeedInterrupt(TaskInfo taskInfo, Map<String, Object> input) {
        SuspenseTask suspenseTask = (SuspenseTask) taskInfo.getTask();
        if (CollectionUtils.isEmpty(suspenseTask.getInterruptions())) {
            return false;
        }
        return conditionsAnyMatch(suspenseTask.getInterruptions(), input, "input");
    }

    private void taskInfoValid(String executionId, String taskInfoName, TaskInfo taskInfo) {
        if (taskInfo == null) {
            log.info("taskInfoValid taskInfo null, executionId:{}, taskInfoName:{}", executionId, taskInfoName);
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("dag %s can not get task %s", executionId, taskInfoName));
        }

        if (!Objects.equals(taskInfo.getTask().getCategory(), TaskCategory.SUSPENSE.getValue())) {
            log.info("taskInfoValid taskInfo type is not suspense, executionId:{}, taskInfoName:{}", executionId, taskInfoName);
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("task %s category is not suspense type", taskInfoName));
        }

        if (taskInfo.getTaskStatus().isCompleted()) {
            log.info("taskInfoValid taskInfo is already complete, executionId:{}, taskInfoName:{}", executionId, taskInfoName);
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), String.format("repeated finish task %s", taskInfoName));
        }
    }
}
