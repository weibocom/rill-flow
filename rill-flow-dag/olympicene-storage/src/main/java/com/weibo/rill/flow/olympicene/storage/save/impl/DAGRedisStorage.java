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

package com.weibo.rill.flow.olympicene.storage.save.impl;

import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DAGRedisStorage implements DAGInfoStorage, DAGContextStorage {
    protected DAGInfoDAO dagInfoDAO;
    protected ContextDAO contextDao;

    public DAGRedisStorage(DAGInfoDAO dagInfoDAO, ContextDAO contextDao) {
        this.dagInfoDAO = dagInfoDAO;
        this.contextDao = contextDao;
    }

    @Override
    public void saveDAGInfo(String executionId, DAGInfo dagInfo) {
        dagInfoDAO.updateDagInfo(executionId, dagInfo);
    }

    @Override
    public void saveTaskInfos(String executionId, Set<TaskInfo> taskInfos) {
        dagInfoDAO.saveTaskInfos(executionId, taskInfos);
    }

    @Override
    public DAGInfo getDAGInfo(String executionId) {
        return dagInfoDAO.getDagInfo(executionId, true);
    }

    @Override
    public DAGInfo getBasicDAGInfo(String executionId) {
        return dagInfoDAO.getDagInfo(executionId, false);
    }

    @Override
    public TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        return dagInfoDAO.getBasicTaskInfo(executionId, taskName);
    }

    @Override
    public TaskInfo getTaskInfo(String executionId, String taskName) {
        return dagInfoDAO.getTaskInfoWithAllSubTask(executionId, taskName);
    }

    @Override
    public TaskInfo getParentTaskInfoWithSibling(String executionId, String taskName) {
        return dagInfoDAO.getParentTaskInfoWithSibling(executionId, taskName);
    }

    @Override
    public void updateContext(String executionId, Map<String, Object> context) {
        contextDao.updateContext(executionId, context);
    }

    @Override
    public Map<String, Object> getContext(String executionId) {
        return contextDao.getContext(executionId, false);
    }

    @Override
    public Map<String, Object> getContext(String executionId, Collection<String> fields) {
        return contextDao.getContext(executionId, fields);
    }

    @Override
    public void clearContext(String executionId) {
        contextDao.deleteContext(executionId);
    }

    @Override
    public void clearDAGInfo(String executionId) {
        dagInfoDAO.delDagInfo(executionId);
    }

    @Override
    public void clearDAGInfo(String executionId, int expireTimeInSecond) {
        dagInfoDAO.delDagInfo(executionId, expireTimeInSecond);
    }

    @Override
    public DAG getDAGDescriptor(String executionId) {
        return dagInfoDAO.getDAGDescriptor(executionId);
    }

    @Override
    public void updateDAGDescriptor(String executionId, DAG dag) {
        dagInfoDAO.updateDAGDescriptor(executionId, dag);
    }
}
