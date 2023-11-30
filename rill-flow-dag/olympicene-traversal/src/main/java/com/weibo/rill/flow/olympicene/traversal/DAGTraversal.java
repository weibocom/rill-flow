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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.concurrent.ExecutionRunnable;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.model.task.ForeachTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.helper.PluginHelper;
import com.weibo.rill.flow.olympicene.traversal.helper.Stasher;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * 遍历节点并找到需要执行的任务
 */
@Slf4j
public class DAGTraversal {
    private final ContextHelper contextHelper = ContextHelper.getInstance();

    private final DAGContextStorage dagContextStorage;
    private final DAGInfoStorage dagInfoStorage;
    private final DAGStorageProcedure dagStorageProcedure;
    private final ExecutorService traversalExecutor;
    @Setter
    private DAGOperations dagOperations;
    @Setter
    private Stasher stasher;

    public DAGTraversal(DAGContextStorage dagStorage, DAGInfoStorage dagInfoStorage, DAGStorageProcedure dagStorageProcedure,
                        ExecutorService traversalExecutor) {
        this.dagContextStorage = dagStorage;
        this.dagInfoStorage = dagInfoStorage;
        this.dagStorageProcedure = dagStorageProcedure;
        this.traversalExecutor = traversalExecutor;
    }

    public void submitTraversal(String executionId, String completedTaskName) {
        traversalExecutor.execute(new ExecutionRunnable(executionId,() -> {
            try {
                log.info("submitTraversal begin lock executionId:{}, completedTaskName:{}", executionId, completedTaskName);

                Map<String, Object> params = Maps.newHashMap();
                params.put("executionId", executionId);
                params.put("completedTaskName", completedTaskName);

                Runnable basicActions = () -> dagStorageProcedure.lockAndRun(
                        LockerKey.buildDagInfoLockName(executionId), () -> doTraversal(executionId, completedTaskName));
                Runnable runnable = PluginHelper.pluginInvokeChain(basicActions, params, SystemConfig.TRAVERSAL_CUSTOMIZED_PLUGINS);
                DAGOperations.OPERATE_WITH_RETRY.accept(runnable, SystemConfig.getTraversalRetryTimes());
            } catch (Exception e) {
                log.error("executionId:{} traversal exception with completedTaskName:{}. ", executionId, completedTaskName, e);
            }
        }));
    }

    public void submitTasks(String executionId, Set<TaskInfo> taskInfos, Map<String, Object> groupedContext) {
        traversalExecutor.execute(new ExecutionRunnable(executionId, () -> {
            try {
                log.info("submitTasks begin get lock executionId:{}", executionId);
                Runnable runnable = () -> dagStorageProcedure.lockAndRun(LockerKey.buildDagInfoLockName(executionId), () -> {
                    log.info("submitTasks begin execute task executionId:{}", executionId);
                    Set<TaskInfo> readyToRunTasks = DAGWalkHelper.getInstance().getReadyToRunTasks(taskInfos);
                    if (CollectionUtils.isNotEmpty(readyToRunTasks)) {
                        List<Pair<TaskInfo, Map<String, Object>>> taskToContexts = contextHelper.getContext(readyToRunTasks, groupedContext);
                        runTasks(executionId, taskToContexts);
                    }
                });
                DAGOperations.OPERATE_WITH_RETRY.accept(runnable, SystemConfig.getTraversalRetryTimes());
            } catch (Exception e) {
                log.error("dag {} traversal exception with tasks {}. ", executionId, Joiner.on(",").join(taskInfos.stream().map(TaskInfo::getName).collect(Collectors.toList())), e);
            }
        }));
    }

    public void doTraversal(String executionId, String completedTaskName) {
        log.info("doTraversal start, executionId:{}", executionId);
        if (StringUtils.isEmpty(completedTaskName) || DAGWalkHelper.getInstance().isAncestorTask(completedTaskName)) {
            traversalAncestorTasks(executionId);
        } else {
            traversalNestedTasks(executionId, completedTaskName);
        }
    }

    private void traversalAncestorTasks(String executionId) {
        DAGInfo dagInfo = dagInfoStorage.getBasicDAGInfo(executionId);
        if (dagInfo == null || dagInfo.getDagStatus().isCompleted()) {
            return;
        }

        Set<TaskInfo> readyToRunTasks = DAGWalkHelper.getInstance().getReadyToRunTasks(dagInfo.getTasks().values());
        if (CollectionUtils.isNotEmpty(readyToRunTasks)) {
            List<Pair<TaskInfo, Map<String, Object>>> taskToContexts = contextHelper.getContext(dagContextStorage, executionId, readyToRunTasks);
            runTasks(executionId, taskToContexts);
            return;
        }

        DAGStatus calculatedDAGStatus = DAGWalkHelper.getInstance().calculateDAGStatus(dagInfo);
        if (calculatedDAGStatus.isCompleted()) {
            dagOperations.finishDAG(executionId, dagInfo, calculatedDAGStatus, null);
        }

        if (DAGStatus.KEY_SUCCEED.equals(calculatedDAGStatus)) {
            dagOperations.finishDAG(executionId, dagInfo, calculatedDAGStatus, null);
        }
    }

    private void traversalNestedTasks(String executionId, String completedTaskName) {
        TaskInfo parent = dagInfoStorage.getParentTaskInfoWithSibling(executionId, completedTaskName);
        if (parent == null) {
            return;
        }

        Set<TaskInfo> readyToRunTasks = DAGWalkHelper.getInstance().getReadyToRunTasks(parent.getChildren().values());
        if (CollectionUtils.isNotEmpty(readyToRunTasks)) {
            List<Pair<TaskInfo, Map<String, Object>>> taskToContexts = contextHelper.getContext(dagContextStorage, executionId, readyToRunTasks);
            runTasks(executionId, taskToContexts);
            return;
        }

        TaskStatus currentGroupStatus = DAGWalkHelper.getInstance().calculateTaskStatus(parent.getChildren().values());
        if (currentGroupStatus.isCompleted()) {
            String groupIndex = DAGWalkHelper.getInstance().getTaskInfoGroupIndex(completedTaskName);
            NotifyInfo notifyInfo = NotifyInfo.builder()
                    .taskInfoName(parent.getName())
                    .completedGroupIndex(groupIndex)
                    .groupTaskStatus(currentGroupStatus)
                    .tasks(parent.getChildren())
                    .build();
            dagOperations.finishTaskAsync(executionId, parent.getTask().getCategory(), notifyInfo, new HashMap<>());
        }
    }

    private void runTasks(String executionId, List<Pair<TaskInfo, Map<String, Object>>> taskInfoToContexts) {
        // 若此处不设置为READY 则同一个任务可能触发多次执行
        taskInfoToContexts.forEach(taskInfoMapPair -> {
            TaskInfo taskInfo = taskInfoMapPair.getLeft();
            if (stasher.needStash(executionId, taskInfo, taskInfoMapPair.getRight())) {
                taskInfo.setTaskStatus(TaskStatus.STASHED);
                String groupIndex = DAGWalkHelper.getInstance().getTaskInfoGroupIndex(taskInfo.getName());
                if (groupIndex != null) {
                    Optional.ofNullable(taskInfo.getParent())
                            .filter(it -> it.getTask() instanceof ForeachTask)
                            .ifPresent(it -> it.getSubGroupIndexToStatus().put(groupIndex, TaskStatus.STASHED));
                }
            } else {
                taskInfo.setTaskStatus(TaskStatus.READY);
            }
        });

        Set<TaskInfo> readyToRunTasks = taskInfoToContexts.stream()
                .map(Pair::getLeft)
                .collect(Collectors.toSet());
        dagInfoStorage.saveTaskInfos(executionId, readyToRunTasks);

        Map<TaskStatus, List<Pair<TaskInfo, Map<String, Object>>>> classifiedTaskInfoToContexts = taskInfoToContexts.stream().collect(Collectors.groupingBy(it -> it.getLeft().getTaskStatus()));

        // 1. stash
        Optional.ofNullable(classifiedTaskInfoToContexts.get(TaskStatus.STASHED)).orElse(new ArrayList<>())
                .forEach(it -> stasher.stash(executionId, it));
        // 2. run
        Optional.ofNullable(classifiedTaskInfoToContexts.get(TaskStatus.READY)).filter(CollectionUtils::isNotEmpty)
                .ifPresent(it -> dagOperations.runTasks(executionId, it));
    }
}
