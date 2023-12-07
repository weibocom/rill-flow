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

package com.weibo.rill.flow.olympicene.core.model.dag;

import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Getter
@Setter
public class DAGInfo {
    private String executionId;
    private DAG dag;
    private DAGInvokeMsg dagInvokeMsg;
    private DAGStatus dagStatus;
    private Map<String, TaskInfo> tasks = new LinkedHashMap<>();

    @Override
    public String toString() {
        return "DAGInfo{" +
                "executionId='" + executionId + '\'' +
                ", dag=" + dag +
                ", dagStatus=" + dagStatus +
                ", tasks=" + tasks +
                '}';
    }

    public void updateInvokeMsg(DAGInvokeMsg dagInvokeMsg) {
        if (dagInvokeMsg == null || this.dagInvokeMsg == dagInvokeMsg) {
            return;
        }

        if (this.dagInvokeMsg == null) {
            this.dagInvokeMsg = dagInvokeMsg;
        } else {
            this.dagInvokeMsg.updateInvokeMsg(dagInvokeMsg);
        }
    }

    public void updateInvokeMsg() {
        if (dagInvokeMsg == null) {
            dagInvokeMsg = new DAGInvokeMsg();
        }
        Optional.ofNullable(DAGWalkHelper.getInstance().getFailedTasks(this))
                .filter(CollectionUtils::isNotEmpty)
                .map(it -> it.get(0))
                .map(TaskInfo::getTaskInvokeMsg)
                .ifPresent(failedTaskInvokeMsg -> dagInvokeMsg.updateInvokeMsg(failedTaskInvokeMsg));
    }

    public void update(DAGInfo dagInfo) {
        if (dagInfo == null) {
            return;
        }

        Optional.ofNullable(dagInfo.getExecutionId()).ifPresent(this::setExecutionId);
        Optional.ofNullable(dagInfo.getDag()).ifPresent(this::setDag);
        Optional.ofNullable(dagInfo.getDagInvokeMsg()).ifPresent(this::setDagInvokeMsg);
        Optional.ofNullable(dagInfo.getDagStatus()).ifPresent(this::setDagStatus);
        if (this.tasks == null) {
            this.tasks = new LinkedHashMap<>();
        }
        Optional.ofNullable(dagInfo.getTasks()).filter(MapUtils::isNotEmpty)
                .ifPresent(taskInfos -> taskInfos.forEach((taskName, taskInfo) -> {
                    if (this.tasks.containsKey(taskName)) {
                        this.tasks.get(taskName).update(taskInfo);
                    } else {
                        this.tasks.put(taskName, taskInfo);
                    }
                }));
    }

    public TaskInfo getTask(String taskName) {
        return tasks.get(taskName);
    }

    public void setTask(String taskName, TaskInfo taskInfo) {
        tasks.put(taskName, taskInfo);
    }

    public static DAGInfo cloneToSave(DAGInfo dagInfo) {
        if (dagInfo == null) {
            return null;
        }

        DAGInfo dagInfoClone = new DAGInfo();
        dagInfoClone.setExecutionId(dagInfo.getExecutionId());
        dagInfoClone.setDag(dagInfo.getDag());
        dagInfoClone.setDagInvokeMsg(DAGInvokeMsg.cloneToSave(dagInfo.getDagInvokeMsg()));
        dagInfoClone.setDagStatus(dagInfo.getDagStatus());
        Map<String, TaskInfo> tasks = new LinkedHashMap<>();
        if (MapUtils.isNotEmpty(dagInfo.getTasks())) {
            dagInfo.getTasks().forEach((taskName, taskInfo) -> tasks.put(taskName, TaskInfo.cloneToSave(taskInfo)));
        }
        dagInfoClone.setTasks(tasks);
        return dagInfoClone;
    }
}
