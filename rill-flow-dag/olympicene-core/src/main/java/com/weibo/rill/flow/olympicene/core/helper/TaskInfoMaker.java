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

import com.weibo.rill.flow.olympicene.core.constant.CoreErrorCode;
import com.weibo.rill.flow.interfaces.model.exception.DAGException;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class TaskInfoMaker {

    private static final TaskInfoMaker INSTANCE = new TaskInfoMaker();
    public static final String COMMA = ",";

    private TaskInfoMaker() {
    }

    public static TaskInfoMaker getMaker() {
        return INSTANCE;
    }

    public Map<String, TaskInfo> makeTaskInfos(List<BaseTask> baseTaskList) {
        return makeTaskInfos(baseTaskList, null, null);
    }

    public Map<String, TaskInfo> makeTaskInfos(List<BaseTask> baseTaskList, TaskInfo parent, Integer index) {
        if (CollectionUtils.isEmpty(baseTaskList)) {
            return new LinkedHashMap<>();
        }

        Map<String, TaskInfo> taskInfoMap = baseTaskList.stream()
                .filter(Objects::nonNull)
                .map(baseTask -> makeTaskInfo(baseTask, index, parent))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(TaskInfo::getName, it -> it));

        appendNextAndDependencyTask(taskInfoMap);
        return taskInfoMap;
    }

    public void appendNextAndDependencyTask(Map<String, TaskInfo> taskInfoMap) {
        taskInfoMap.values().stream()
                .filter(taskInfo -> taskInfo.getTask() != null && StringUtils.isNotEmpty(taskInfo.getTask().getNext()))
                .forEach(taskInfo -> Arrays.stream(taskInfo.getTask().getNext().split(COMMA))
                        .map(baseTaskNext -> DAGWalkHelper.getInstance().buildTaskInfoName(taskInfo.getRouteName(), baseTaskNext))
                        .map(taskInfoMap::get)
                        .filter(Objects::nonNull)
                        .forEach(nextTaskInfo -> {
                            if (taskInfo.getNext() == null) {
                                taskInfo.setNext(new LinkedList<>());
                            }
                            taskInfo.getNext().add(nextTaskInfo);

                            if (nextTaskInfo.getDependencies() == null) {
                                nextTaskInfo.setDependencies(new LinkedList<>());
                            }
                            nextTaskInfo.getDependencies().add(taskInfo);
                        })
                );
    }

    public TaskInfo makeTaskInfo(BaseTask task, Integer index, TaskInfo parent) {
        return makeTaskInfo(task, index, null, parent, null);
    }

    /**
     * routeName = parentTaskName + "_" + index
     * taskName = routeName + "-" + baseTaskName
     */
    public TaskInfo makeTaskInfo(BaseTask baseTask, Integer index, List<TaskInfo> next, TaskInfo parent, Map<String, TaskInfo> children) {
        if (index == null && parent != null || index != null && parent == null) {
            throw new DAGException(CoreErrorCode.TASK_ILLEGAL_STATE.getCode(), CoreErrorCode.TASK_ILLEGAL_STATE.getMessage());
        }

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTask(baseTask);
        taskInfo.setRouteName(DAGWalkHelper.getInstance().buildTaskInfoRouteName(Optional.ofNullable(parent).map(TaskInfo::getName).orElse(null), String.valueOf(index)));
        taskInfo.setName(DAGWalkHelper.getInstance().buildTaskInfoName(taskInfo.getRouteName(), baseTask.getName()));
        taskInfo.setTaskStatus(TaskStatus.NOT_STARTED);
        taskInfo.setParent(parent);
        Optional.ofNullable(next).ifPresent(taskInfo::setNext);
        Optional.ofNullable(children).ifPresent(taskInfo::setChildren);

        return taskInfo;
    }
}
