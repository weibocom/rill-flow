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

    private static final Set<String> FORK_TASK_CATEGORIES = Set.of(TaskCategory.SWITCH.getValue(), TaskCategory.RETURN.getValue(),
            TaskCategory.FOREACH.getValue(), TaskCategory.CHOICE.getValue());

    private DAGWalkHelper() {
        // do nothing
    }

    public static DAGWalkHelper getInstance() {
        return INSTANCE;
    }

    public Set<TaskInfo> getReadyToRunTasks(Collection<TaskInfo> taskInfos) {
        boolean hasStreamInputTask = taskInfos.stream()
                .map(TaskInfo::getTask)
                .filter(Objects::nonNull)
                .anyMatch(task -> TaskInputType.getInputTypeByValue(task.getInputType()) == TaskInputType.STREAM);
        boolean isKeyMode = isKeyMode(taskInfos);

        // 筛选出准备运行的任务:
        // 1. 任务不为空且状态为未开始
        // 2. 所有依赖任务都已成功或跳过
        // 3. 如果是关键路径回调任务，则只在关键路径模式下运行
        Set<TaskInfo> readyToRunTasks = taskInfos.stream()
                .filter(taskInfo -> taskInfo != null && taskInfo.getTaskStatus() == TaskStatus.NOT_STARTED)
                .filter(taskInfo -> isDependenciesAllSuccessOrSkip(taskInfo, hasStreamInputTask, isKeyMode))
                .collect(Collectors.toSet());

        // 如果存在 stream 输入任务，则找到所有可以运行的 stream 输入任务
        if (hasStreamInputTask) {
            readyToRunTasks.addAll(findStreamInputTasksCanRun(readyToRunTasks));
        }
        return readyToRunTasks;
    }

    private Collection<TaskInfo> findStreamInputTasksCanRun(Set<TaskInfo> readyToRunTasks) {
        Map<String, TaskInfo> streamInputTaskInfoMap = new HashMap<>();
        Set<String> skipTaskNames = Sets.newHashSet();
        for (TaskInfo taskInfo : readyToRunTasks) {
            findNextStreamInputTask(taskInfo, streamInputTaskInfoMap, skipTaskNames);
        }
        return streamInputTaskInfoMap.values();
    }

    /**
     * 找到当前节点路径上的下一个可以被执行的 stream 输入节点
     * @param taskInfo 当前节点
     * @param streamInputTaskInfoMap 作为返回的直结果参数
     * @param skipTaskNames 已经处理过的任务名称，用于去重，避免重复处理
     */
    private void findNextStreamInputTask(TaskInfo taskInfo, Map<String, TaskInfo> streamInputTaskInfoMap, Set<String> skipTaskNames) {
        List<TaskInfo> nextTaskInfos = taskInfo.getNext();
        String category = taskInfo.getTask().getCategory();
        TaskInputType inputType = TaskInputType.getInputTypeByValue(taskInfo.getTask().getInputType());
        
        if (shouldSkipTask(taskInfo, nextTaskInfos, category, inputType)) {
            return;
        }
        
        for (TaskInfo nextTaskInfo : nextTaskInfos) {
            processNextTaskInfo(nextTaskInfo, streamInputTaskInfoMap, skipTaskNames);
        }
    }

    private boolean shouldSkipTask(TaskInfo taskInfo, List<TaskInfo> nextTaskInfos, String category, TaskInputType inputType) {
        return CollectionUtils.isEmpty(nextTaskInfos) || FORK_TASK_CATEGORIES.contains(category)
                || (inputType == TaskInputType.STREAM && taskInfo.getTaskStatus() == TaskStatus.NOT_STARTED);
    }

    private void processNextTaskInfo(TaskInfo nextTaskInfo, Map<String, TaskInfo> streamInputTaskInfoMap, Set<String> skipTaskNames) {
        String nextTaskName = nextTaskInfo.getName();
        String nextCategory = nextTaskInfo.getTask().getCategory();
        TaskInputType nextInputType = TaskInputType.getInputTypeByValue(nextTaskInfo.getTask().getInputType());

        if (skipTaskNames.contains(nextTaskName) || FORK_TASK_CATEGORIES.contains(nextCategory)) {
            return;
        }
        skipTaskNames.add(nextTaskName);
        if (nextInputType == TaskInputType.STREAM && nextTaskInfo.getTaskStatus() == TaskStatus.NOT_STARTED) {
            if (!isDependOnUnfinishedStreamInputTask(nextTaskInfo, new HashSet<>(Set.of(nextTaskName)))) {
                streamInputTaskInfoMap.put(nextTaskInfo.getName(), nextTaskInfo);
            }
            return;
        }
        findNextStreamInputTask(nextTaskInfo, streamInputTaskInfoMap, skipTaskNames);
    }

    /**
     * 判断是否依赖于尚未执行完成的 Stream 输入节点
     * @param taskInfo 任务信息
     * @param skipTaskNames 跳过的任务名称，用于去重避免重复处理
     * @return boolean 类型结果
     */
    private boolean isDependOnUnfinishedStreamInputTask(TaskInfo taskInfo, Set<String> skipTaskNames) {
        // 如果节点不依赖任何节点，返回 false
        if (CollectionUtils.isEmpty(taskInfo.getDependencies())) {
            return false;
        }
        // 如果节点依赖的任务已经被处理过，则直接跳过
        // 如果有任何一个依赖的任务是没有处理完成的 Stream 输入节点，则返回 false
        // 如果是非 Stream 输入节点，则递归调用，直到找到没有处理完成的 Stream 输入节点，或者全部节点都已被处理过
        return taskInfo.getDependencies().stream()
            .filter(dependencyTask -> !skipTaskNames.contains(dependencyTask.getName()))
            .anyMatch(dependencyTask -> {
                skipTaskNames.add(dependencyTask.getName());
                TaskInputType inputType = TaskInputType.getInputTypeByValue(dependencyTask.getTask().getInputType());
                return (inputType == TaskInputType.STREAM && !dependencyTask.getTaskStatus().isSuccessOrSkip())
                    || (inputType != TaskInputType.STREAM && isDependOnUnfinishedStreamInputTask(dependencyTask, skipTaskNames));
            });
    }

    private boolean isDependenciesAllSuccessOrSkip(TaskInfo taskInfo, boolean hasStreamInputTask, boolean isKeyMode) {
        boolean isKeyCallback = taskInfo.getTask().isKeyCallback();
        return isDependenciesAllSuccessOrSkip(taskInfo, hasStreamInputTask, isKeyMode, isKeyCallback);
    }

    /**
     * 判断依赖的所有任务是否都已完成
     * 1. 如果没有依赖，说明依赖的所有任务都已完成
     * 2. 如果依赖的是 Stream 输入类型的节点，那么忽略该 Stream 输入节点，检查 Stream 输入节点的所有依赖是否都已完成
     * 3. 如果依赖的是非 Stream 输入类型的节点，那么检查该非 Stream 输入节点是否已完成
     *
     * @param taskInfo 待处理的任务信息
     * @param hasStreamInputTask 当前所有待处理任务中是否存在 Stream 输入节点
     * @param isKeyMode 当前是否处于关键路径模式
     * @param isKeyCallback 待处理的任务是否是关键路径回调任务
     * @return 返回待处理的任务是否可以被执行
     */
    private boolean isDependenciesAllSuccessOrSkip(TaskInfo taskInfo, boolean hasStreamInputTask, boolean isKeyMode, boolean isKeyCallback) {
        if (!hasStreamInputTask) {
            return CollectionUtils.isEmpty(taskInfo.getDependencies())
                    || taskInfo.getDependencies().stream().allMatch(
                            i -> (i.getTaskStatus().isSuccessOrSkip())
                                    || isKeyMode && isKeyCallback && i.getTaskStatus().isSuccessOrKeySuccessOrSkip());
        }

        return CollectionUtils.isEmpty(taskInfo.getDependencies()) ||
               taskInfo.getDependencies().stream().allMatch(dependency -> {
                   TaskInputType inputType = TaskInputType.getInputTypeByValue(dependency.getTask().getInputType());
                   return inputType == TaskInputType.STREAM && isDependenciesAllSuccessOrSkip(dependency, true, isKeyMode, isKeyCallback)
                           || inputType != TaskInputType.STREAM && dependency.getTaskStatus().isSuccessOrSkip()
                           || isKeyMode && isKeyCallback && dependency.getTaskStatus().isSuccessOrKeySuccessOrSkip();
               });
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
