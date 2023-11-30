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

package com.weibo.rill.flow.interfaces.model.task;


import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.weibo.rill.flow.interfaces.model.exception.DAGException;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Setter
@Getter
@JsonIdentityInfo(
        generator = ObjectIdGenerators.UUIDGenerator.class,
        property = "@json_id"
)
public class TaskInfo {
    private BaseTask task;

    /**
     * 区别于baseTask中的name，这里的name是TaskInfo，可能出现一个baseTask对应多个TaskInfo的情况，
     * 比如foreach场景中，每次遍历的TaskInfo都是不一样的
     */
    private String name;

    /**
     * 一条路径(route)的名字
     */
    private String routeName;

    private TaskStatus taskStatus;

    private Map<String, TaskStatus> subGroupIndexToStatus;

    private Map<String, Boolean> subGroupKeyJudgementMapping;

    private Map<String, String> subGroupIndexToIdentity;

    private TaskInvokeMsg taskInvokeMsg;

    /**
     * 以下为引用数据
     */
    private List<TaskInfo> next = new LinkedList<>();
    private TaskInfo parent;
    private Map<String, TaskInfo> children = new LinkedHashMap<>();
    private List<TaskInfo> dependencies = new LinkedList<>();

    @Override
    public String toString() {
        return "TaskInfo{" +
                "task=" + task +
                ", name='" + name + '\'' +
                ", routeName='" + routeName + '\'' +
                ", taskStatus=" + taskStatus +
                '}';
    }

    public void updateInvokeMsg(TaskInvokeMsg taskInvokeMsg) {
        if (taskInvokeMsg == null || this.taskInvokeMsg == taskInvokeMsg) {
            return;
        }

        if (this.taskInvokeMsg == null) {
            this.taskInvokeMsg = taskInvokeMsg;
        } else {
            this.taskInvokeMsg.updateInvokeMsg(taskInvokeMsg);
        }
    }

    public void update(TaskInfo taskInfo) {
        doUpdate(1, taskInfo);
    }

    public void doUpdate(int depth, TaskInfo taskInfo) {
        if (taskInfo == null) {
            return;
        }

        Optional.ofNullable(taskInfo.getTaskStatus()).ifPresent(this::setTaskStatus);
        Optional.ofNullable(taskInfo.getSubGroupIndexToStatus()).filter(it -> !it.isEmpty()).ifPresent(this::setSubGroupIndexToStatus);
        Optional.ofNullable(taskInfo.getSubGroupKeyJudgementMapping()).filter(it -> !it.isEmpty()).ifPresent(this::setSubGroupKeyJudgementMapping);
        Optional.ofNullable(taskInfo.getSubGroupIndexToIdentity()).filter(it -> !it.isEmpty()).ifPresent(this::setSubGroupIndexToIdentity);
        Optional.ofNullable(taskInfo.getTaskInvokeMsg()).ifPresent(this::setTaskInvokeMsg);
        if (children == null) {
            children = new LinkedHashMap<>();
        }
        Optional.ofNullable(taskInfo.getChildren()).filter(it -> !it.isEmpty())
                .ifPresent(taskInfos -> taskInfos.forEach((taskName, tInfo) -> {
                    if (this.children.containsKey(taskName) && depth <= 3) {
                        this.children.get(taskName).doUpdate(depth + 1, tInfo);
                    } else {
                        this.children.put(taskName, tInfo);
                        tInfo.setParent(this);
                    }
                }));
    }

    public static TaskInfo cloneToSave(TaskInfo taskInfo) {
        // 设计上要求taskName不重复
        // 添加该set 避免循环引用导致递归无法退出
        Set<String> allTaskNames = new HashSet<>();
        return doCloneToSave(taskInfo, allTaskNames);
    }

    private static TaskInfo doCloneToSave(TaskInfo taskInfo, Set<String> allTaskNames) {
        if (taskInfo == null) {
            return null;
        }

        String taskName = taskInfo.getName();
        if (allTaskNames.contains(taskName)) {
            throw new DAGException(-1, "name duplicated: " + taskName);
        }
        allTaskNames.add(taskName);

        TaskInfo taskInfoClone = new TaskInfo();
        taskInfoClone.setName(taskInfo.getName());
        taskInfoClone.setRouteName(taskInfo.getRouteName());
        taskInfoClone.setTaskStatus(taskInfo.getTaskStatus());
        taskInfoClone.setSubGroupIndexToStatus(taskInfo.getSubGroupIndexToStatus());
        taskInfoClone.setSubGroupKeyJudgementMapping(taskInfo.getSubGroupKeyJudgementMapping());
        taskInfoClone.setSubGroupIndexToIdentity(taskInfo.getSubGroupIndexToIdentity());
        taskInfoClone.setTaskInvokeMsg(taskInfo.getTaskInvokeMsg());
        Map<String, TaskInfo> children = new LinkedHashMap<>();
        if (taskInfo.getChildren() != null && !taskInfo.getChildren().isEmpty()) {
            taskInfo.getChildren().forEach((name, task) -> children.put(name, doCloneToSave(task, allTaskNames)));
        }
        taskInfoClone.setChildren(children);
        return taskInfoClone;
    }
}
