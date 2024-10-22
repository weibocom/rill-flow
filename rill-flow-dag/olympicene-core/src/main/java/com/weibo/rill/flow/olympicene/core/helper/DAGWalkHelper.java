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

package com.weibo.rill.flow.olympicene.core.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.interfaces.model.task.*;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 2021-03-16 暂时最多支持三层嵌套(包含最外层的DAG在内一共三层)
 */
@Slf4j
public class DAGWalkHelper {

    private static final DAGWalkHelper INSTANCE = new DAGWalkHelper();

    private DAGWalkHelper() {
        // do nothing
    }

    public static DAGWalkHelper getInstance() {
        return INSTANCE;
    }

    public Set<TaskInfo> getReadyToRunTasks(Collection<TaskInfo> taskInfos) {
        boolean isKeyMode = isKeyMode(taskInfos);

        // 筛选出准备运行的任务:
        // 1. 任务不为空且状态为未开始
        // 2. 所有依赖的 block 输出任务都已成功或跳过
        // 3. 如果是关键路径回调任务，并且在关键路径模式下，则只要关键路径完成执行就可以运行
        Set<TaskInfo> readyToRunTasks = taskInfos.stream()
                .filter(taskInfo -> taskInfo != null && taskInfo.getTaskStatus() == TaskStatus.NOT_STARTED)
                .filter(taskInfo -> isDependenciesAllSuccessOrSkip(taskInfo, isKeyMode))
                .collect(Collectors.toSet());

        // 4. 如果 stream 输入的任务依赖的任意 stream 输出任务在待执行列表中，则该 stream 输入任务也可以执行
        readyToRunTasks.addAll(getReadyToRunStreamInputTasks(taskInfos, readyToRunTasks));
        return readyToRunTasks;
    }

    /**
     * 判断依赖的所有任务是否都已完成
     * 1. 如果没有依赖，说明依赖的所有任务都已完成
     * 2. 流式输入任务，有任意依赖的 block 输出任务完成或关键路径下完成
     * 3. 非流式输入任务，所有依赖的 block 输出任务是否都已经完成，或在关键路径模式下关键路径完成
     */
    private boolean isDependenciesAllSuccessOrSkip(TaskInfo taskInfo, boolean isKeyMode) {
        // 1. 没有依赖视为依赖均已完成
        if (CollectionUtils.isEmpty(taskInfo.getDependencies())) {
            return true;
        }
        TaskInputOutputType inputType = TaskInputOutputType.getTypeByValue(taskInfo.getTask().getInputType());
        boolean isKeyCallback = taskInfo.getTask().isKeyCallback();
        if (inputType == TaskInputOutputType.STREAM) {
            // 2. 流式输入任务，任意依赖的非流式任务已完成（非关键路径模式下完成或跳过，或者在关键路径模式下关键路径完成或跳过）
            return taskInfo.getDependencies().stream().anyMatch(dependency -> {
                TaskInputOutputType dependencyOutputType = TaskInputOutputType.getTypeByValue(dependency.getTask().getOutputType());
                return dependencyOutputType == TaskInputOutputType.STREAM && dependency.getTaskStatus() != TaskStatus.NOT_STARTED
                        || dependencyOutputType == TaskInputOutputType.BLOCK && isTaskSuccessOrSkip(dependency, isKeyMode, isKeyCallback);
            });
        } else {
            // 3. 非流式输入任务，所有依赖任务是否都已完成（非关键路径模式下完成或跳过，或者在关键路径模式下关键路径完成或跳过）
            return taskInfo.getDependencies().stream().allMatch(dependency -> isTaskSuccessOrSkip(dependency, isKeyMode, isKeyCallback));
        }
    }

    private boolean isTaskSuccessOrSkip(TaskInfo taskInfo, boolean isKeyMode, boolean isKeyCallback) {
        return taskInfo.getTaskStatus().isSuccessOrSkip()
                || (isKeyMode && isKeyCallback && taskInfo.getTaskStatus().isSuccessOrKeySuccessOrSkip());
    }

    /**
     * 获取准备运行的流式输入任务
     * 
     * @param taskInfos 所有任务的集合
     * @param readyToRunTasks 已准备运行的任务集合
     * @return 准备运行的流输入任务集合
     */
    private Set<TaskInfo> getReadyToRunStreamInputTasks(Collection<TaskInfo> taskInfos, Set<TaskInfo> readyToRunTasks) {
        Set<TaskInfo> readyToRunStreamTasks = new HashSet<>();
        taskInfos.stream().filter(Objects::nonNull).filter(taskInfo -> taskInfo.getTaskStatus() == TaskStatus.NOT_STARTED)
                .filter(taskInfo -> TaskInputOutputType.getTypeByValue(taskInfo.getTask().getInputType()) == TaskInputOutputType.STREAM)
                .forEach(taskInfo -> {
                    boolean needRun = taskInfo.getDependencies().stream().anyMatch(dependency -> {
                        TaskInputOutputType dependencyOutputType = TaskInputOutputType.getTypeByValue(dependency.getTask().getOutputType());
                        // 如果依赖任务是流输出类型且已开始执行，或者准备运行，则将当前任务添加到准备运行的流任务集合中
                        return dependencyOutputType == TaskInputOutputType.STREAM && dependency.getTaskStatus() != TaskStatus.NOT_STARTED
                                || readyToRunTasks.contains(dependency);
                    });
                    if (needRun) {
                        readyToRunStreamTasks.add(taskInfo);
                    }
                });
        return readyToRunStreamTasks;
    }

    private boolean isKeyMode(Collection<TaskInfo> allTasks) {
        return allTasks.stream().map(TaskInfo::getTaskStatus).anyMatch(TaskStatus::isKeyModeStatus);
    }

    public TaskStatus calculateTaskStatus(Collection<TaskInfo> taskInfos) {
        if (CollectionUtils.isEmpty(taskInfos) ||
                taskInfos.stream().allMatch(taskInfo -> taskInfo.getTaskStatus().isSuccessOrSkip())) {
            return TaskStatus.SUCCEED;
        }
        if (taskInfos.stream().anyMatch(taskInfo -> taskInfo.getTaskStatus().isFailed())) {
            return TaskStatus.FAILED;
        }

        if (taskInfos.stream()
                .anyMatch(taskInfo -> taskInfo.getTaskStatus() == TaskStatus.RUNNING ||
                        taskInfo.getTaskStatus() == TaskStatus.READY)) {
            return TaskStatus.RUNNING;
        }

        return TaskStatus.NOT_STARTED;
    }

    public TaskStatus calculateParentStatus(TaskInfo parentTask) {
        if (!Objects.equals(parentTask.getTask().getCategory(), TaskCategory.CHOICE.getValue())
                && !Objects.equals(parentTask.getTask().getCategory(), TaskCategory.FOREACH.getValue())) {
            return parentTask.getTaskStatus();
        }

        Map<String, TaskStatus> subGroupIndexToStatus = parentTask.getSubGroupIndexToStatus();
        if (MapUtils.isEmpty(subGroupIndexToStatus) ||
                subGroupIndexToStatus.values().stream().allMatch(TaskStatus::isSuccessOrSkip)) {
            return TaskStatus.SUCCEED;
        }

        if (isForeachTaskKeySucceed(parentTask)){
            return TaskStatus.KEY_SUCCEED;
        }
        if (subGroupIndexToStatus.values().stream().anyMatch(it -> it == TaskStatus.RUNNING || it == TaskStatus.READY)) {
            return TaskStatus.RUNNING;
        }
        if (subGroupIndexToStatus.values().stream().anyMatch(TaskStatus::isFailed)) {
            return TaskStatus.FAILED;
        }

        return parentTask.getTaskStatus();
    }

    private boolean isForeachTaskKeySucceed(TaskInfo foreachTaskInfo) {
        Map<String, TaskStatus> subGroupIndexToStatus = foreachTaskInfo.getSubGroupIndexToStatus();
        Map<String, Boolean> subGroupKeyJudgementMapping = foreachTaskInfo.getSubGroupKeyJudgementMapping();
        List<String> keyIdxes = Optional.ofNullable(subGroupKeyJudgementMapping).orElse(new HashMap<>())
                .entrySet().stream()
                .filter(it -> it.getValue().equals(true))
                .map(Map.Entry::getKey)
                .toList();
        boolean keyAllCompleted = keyIdxes.stream()
                .map(subGroupIndexToStatus::get)
                .allMatch(taskStatus -> taskStatus != null && taskStatus.isSuccessOrKeySuccessOrSkip());
        return CollectionUtils.isNotEmpty(keyIdxes) && keyAllCompleted;
    }

    public DAGStatus calculateDAGStatus(DAGInfo dagInfo) {
        Collection<TaskInfo> taskInfos = dagInfo.getTasks().values();

        List<String> runnableTaskNames = getReadyToRunTasks(taskInfos).stream().map(TaskInfo::getName).toList();
        List<String> runningTaskNames = taskInfos.stream()
                .filter(taskInfo -> taskInfo.getTaskStatus() == TaskStatus.RUNNING || taskInfo.getTaskStatus() == TaskStatus.READY)
                .map(TaskInfo::getName)
                .toList();

        if (isKeyMode(taskInfos)
                && CollectionUtils.isEmpty(runnableTaskNames)
                && CollectionUtils.isEmpty(runningTaskNames)
                && taskInfos.stream().noneMatch(taskInfo -> taskInfo.getTaskStatus().isFailed())) {
            return DAGStatus.KEY_SUCCEED;
        }

        if (CollectionUtils.isNotEmpty(runnableTaskNames) || CollectionUtils.isNotEmpty(runningTaskNames)) {
            log.info("getDAGStatus dag has runnable task {}, running task {}", runnableTaskNames, runningTaskNames);
            return DAGStatus.RUNNING;
        }

        if (taskInfos.stream().anyMatch(taskInfo -> taskInfo.getTaskStatus().isFailed())) {
            return DAGStatus.FAILED;
        }

        if (taskInfos.stream().allMatch(taskInfo -> taskInfo.getTaskStatus().isSuccessOrSkip())) {
            return DAGStatus.SUCCEED;
        }

        return dagInfo.getDagStatus();
    }

    public TaskInfo getTaskInfoByName(DAGInfo dagInfo, String taskName) {
        if (StringUtils.isEmpty(taskName)) {
            return null;
        }
        return getTaskInfoByName(dagInfo.getTasks(), taskName, 1);
    }

    private TaskInfo getTaskInfoByName(Map<String, TaskInfo> taskInfos, String taskName, int depth) {
        if (depth > SystemConfig.getTaskMaxDepth()) {
            return null;
        }

        TaskInfo result = taskInfos.get(taskName);

        if (result == null) {
            result = taskInfos.values().stream()
                    .map(TaskInfo::getChildren)
                    .filter(MapUtils::isNotEmpty)
                    .map(it -> getTaskInfoByName(it, taskName, depth + 1))
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElse(null);
        }
        return result;
    }

    public boolean isAncestorTask(String taskInfoName) {
        return !taskInfoName.contains(ReservedConstant.ROUTE_NAME_CONNECTOR);
    }

    public String getAncestorTaskName(String taskName) {
        if (StringUtils.isEmpty(taskName)) {
            return taskName;
        }
        return taskName.split(ReservedConstant.ROUTE_NAME_CONNECTOR)[0];
    }

    public String getBaseTaskName(TaskInfo taskInfo) {
        return getBaseTaskName(taskInfo.getName());
    }

    public String getBaseTaskName(String taskInfoName) {
        if (StringUtils.isEmpty(taskInfoName)) {
            return null;
        }

        int index = taskInfoName.lastIndexOf(ReservedConstant.TASK_NAME_CONNECTOR);
        return index < 0 ? taskInfoName : taskInfoName.substring(index + 1);
    }

    public String getTaskInfoGroupIndex(String taskInfoName) {
        if (StringUtils.isEmpty(taskInfoName)) {
            return null;
        }

        int routeConnectorIndex = taskInfoName.lastIndexOf(ReservedConstant.ROUTE_NAME_CONNECTOR);
        int taskConnectorIndex = taskInfoName.lastIndexOf(ReservedConstant.TASK_NAME_CONNECTOR);
        return routeConnectorIndex < 0 || taskConnectorIndex < 0 ? null : taskInfoName.substring(routeConnectorIndex + 1, taskConnectorIndex);
    }

    public String getRootName(String taskInfoName) {
        if (StringUtils.isEmpty(taskInfoName)) {
            return null;
        }

        int index = taskInfoName.lastIndexOf(ReservedConstant.TASK_NAME_CONNECTOR);
        return index < 0 ? null : taskInfoName.substring(0, index);
    }

    public List<String> taskInfoNamesCurrentChain(String name) {
        List<String> chainNames = Lists.newArrayList();
        for (int i = 0; i < name.length(); i++) {
            if (name.charAt(i) == ReservedConstant.ROUTE_NAME_CONNECTOR.charAt(0)) {
                chainNames.add(name.substring(0, i));
            }
        }
        chainNames.add(name);
        return chainNames;
    }

    public String buildTaskInfoRouteName(String parentName, String groupIndex) {
        return parentName == null ? null : parentName + ReservedConstant.ROUTE_NAME_CONNECTOR + groupIndex;
    }

    public String buildTaskInfoName(String routeName, String baseTaskName) {
        return baseTaskName == null ? null : Optional.ofNullable(routeName).map(it -> it + ReservedConstant.TASK_NAME_CONNECTOR + baseTaskName).orElse(baseTaskName);
    }

    public Set<String> buildSubTaskContextFieldNameInCurrentTask(TaskInfo parentTaskInfo) {
        if (MapUtils.isEmpty(parentTaskInfo.getSubGroupIndexToStatus())) {
            return Sets.newHashSet();
        }

        return parentTaskInfo.getSubGroupIndexToStatus().keySet().stream()
                .map(groupIndex -> buildSubTaskContextFieldName(buildTaskInfoRouteName(parentTaskInfo.getName(), groupIndex)))
                .collect(Collectors.toSet());
    }

    public Set<String> buildSubTaskContextFieldName(Collection<TaskInfo> taskInfos) {
        return taskInfos.stream()
                .map(taskInfo -> buildSubTaskContextFieldName(taskInfo.getRouteName()))
                .collect(Collectors.toSet());
    }

    public String buildSubTaskContextFieldName(String taskInfoRouteName) {
        if (StringUtils.isEmpty(taskInfoRouteName)) {
            return null;
        }
        return ReservedConstant.SUB_CONTEXT_PREFIX + taskInfoRouteName;
    }

    public boolean isSubContextFieldName(String fieldName) {
        return fieldName.startsWith(ReservedConstant.SUB_CONTEXT_PREFIX);
    }

    public List<TaskInfo> getFailedTasks(DAGInfo dagInfo) {
        return getFailedTasks(1, dagInfo.getTasks());
    }

    public List<TaskInfo> getFailedTasks(Map<String, TaskInfo> tasks) {
        return getFailedTasks(1, tasks);
    }

    private List<TaskInfo> getFailedTasks(int depth, Map<String, TaskInfo> tasks) {
        List<TaskInfo> failedTasks = Lists.newArrayList();
        if (depth > SystemConfig.getTaskMaxDepth() || MapUtils.isEmpty(tasks)) {
            return failedTasks;
        }

        tasks.values().stream()
                .filter(taskInfo -> taskInfo.getTaskStatus() == TaskStatus.FAILED)
                .forEach(failedTasks::add);
        tasks.values().stream()
                .map(TaskInfo::getChildren)
                .filter(MapUtils::isNotEmpty)
                .forEach(it -> failedTasks.addAll(getFailedTasks(depth + 1, it)));

        return failedTasks;
    }

    public Map<String, List<String>> getDependedResources(DAG dag) {
        Map<String, List<String>> resourceToTaskNameMap = Maps.newHashMap();
        getDependedResources(1, resourceToTaskNameMap, dag.getTasks());
        Optional.ofNullable(dag.getCallbackConfig()).map(CallbackConfig::getResourceName).ifPresent(resourceName -> {
            List<String> names = resourceToTaskNameMap.computeIfAbsent(resourceName, it -> Lists.newArrayList());
            names.add("flow_completed_callback");
        });
        return resourceToTaskNameMap;
    }

    private void getDependedResources(int depth, Map<String, List<String>> resourceToTaskNameMap, List<BaseTask> tasks) {
        if (depth > SystemConfig.getTaskMaxDepth() || CollectionUtils.isEmpty(tasks)) {
            return;
        }

        tasks.stream()
                .filter(task -> task instanceof FunctionTask)
                .map(task -> (FunctionTask) task)
                .forEach(task -> {
                    List<String> taskNames = resourceToTaskNameMap.computeIfAbsent(task.getResourceName(), it -> Lists.newArrayList());
                    taskNames.add(task.getName());
                });
        tasks.stream()
                .map(BaseTask::subTasks)
                .filter(CollectionUtils::isNotEmpty)
                .forEach(it -> getDependedResources(depth + 1, resourceToTaskNameMap, it));
    }
}
