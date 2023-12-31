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

package com.weibo.rill.flow.olympicene.core.runtime;


import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;

import java.util.Set;

public interface DAGInfoStorage {

    void saveDAGInfo(String executionId, DAGInfo dagInfo);

    void saveTaskInfos(String executionId, Set<TaskInfo> taskInfos);

    DAGInfo getDAGInfo(String executionId);

    /**
     * 获取dag基础信息(不返回子任务)
     */
    DAGInfo getBasicDAGInfo(String executionId);

    /**
     * 获取taskInfo基础信息(不返回子任务)
     */
    TaskInfo getBasicTaskInfo(String executionId, String taskName);

    /**
     * 获取当前任务及其子任务
     */
    TaskInfo getTaskInfo(String executionId, String taskName);

    TaskInfo getParentTaskInfoWithSibling(String executionId, String taskName);

    void clearDAGInfo(String executionId);

    void clearDAGInfo(String executionId, int expireTimeInSecond);

    public DAG getDAGDescriptor(String executionId);

    public void updateDAGDescriptor(String executionId, DAG dag);
}
