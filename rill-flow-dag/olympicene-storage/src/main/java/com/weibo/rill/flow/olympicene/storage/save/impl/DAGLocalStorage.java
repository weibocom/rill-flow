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

import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.exception.StorageException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DAGLocalStorage implements DAGInfoStorage, DAGContextStorage {
    private final Map<String, DAGInfo> dagInfoCache = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Object>> contextCache = new ConcurrentHashMap<>();

    public DAGLocalStorage() {
    }

    @Override
    public void saveDAGInfo(String executionId, DAGInfo dagInfo) {
        dagInfoCache.put(executionId, dagInfo);
    }

    @Override
    public void saveTaskInfos(String executionId, Set<TaskInfo> taskInfos) {
        // memory reference value already update
    }

    @Override
    public DAGInfo getDAGInfo(String executionId) {
        return dagInfoCache.get(executionId);
    }

    @Override
    public DAGInfo getBasicDAGInfo(String executionId) {
        return getDAGInfo(executionId);
    }

    @Override
    public TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        return getTaskInfo(executionId, taskName);
    }

    @Override
    public TaskInfo getTaskInfo(String executionId, String taskName) {
        DAGInfo dagInfo = getDAGInfo(executionId);
        return DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskName);
    }

    @Override
    public TaskInfo getParentTaskInfoWithSibling(String executionId, String taskName) {
        String taskRoutName = DAGWalkHelper.getInstance().getRootName(taskName);
        List<String> chainNames = DAGWalkHelper.getInstance().taskInfoNamesCurrentChain(taskName);
        if (chainNames.size() < 2) {
            return null;
        }
        DAGInfo dagInfo = getDAGInfo(executionId);
        TaskInfo parentTaskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, chainNames.get(chainNames.size() - 2));

        Map<String, TaskInfo> sibling = parentTaskInfo.getChildren().entrySet().stream()
                .filter(entry -> entry.getValue().getRouteName().equals(taskRoutName))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        TaskInfo parentClone = TaskInfo.cloneToSave(parentTaskInfo);
        parentClone.setTask(parentTaskInfo.getTask());
        parentClone.setChildren(sibling);
        return parentClone;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateContext(String executionId, Map<String, Object> context) {
        Map<String, Object> cachedContext = contextCache.computeIfAbsent(executionId, k -> Maps.newHashMap());
        context.forEach((key, value) -> {
            if (!key.startsWith(ReservedConstant.SUB_CONTEXT_PREFIX)) {
                cachedContext.put(key, value);
                return;
            }

            if (!(value instanceof Map)) {
                throw new StorageException(StorageErrorCode.CLASS_TYPE_NONSUPPORT.getCode(), "value type is not map");
            }
            ((Map<String, Object>) cachedContext.computeIfAbsent(key, k -> Maps.newHashMap())).putAll((Map<String, Object>) value);
        });
    }

    @Override
    public Map<String, Object> getContext(String executionId) {
        return getContext(executionId, false);
    }

    @Override
    public Map<String, Object> getContext(String executionId, Collection<String> fields) {
        Map<String, Object> internalMap = getContext(executionId, true);
        if (MapUtils.isEmpty(internalMap) || CollectionUtils.isEmpty(fields)) {
            return internalMap;
        }
        Map<String, Object> ret = Maps.newHashMap();
        fields.forEach(field ->
                Optional.ofNullable(internalMap.get(field)).ifPresent(value -> ret.put(field, value)));
        return ret;
    }

    private Map<String, Object> getContext(String executionId, boolean withSubContext) {
        Map<String, Object> context = Optional.ofNullable(contextCache.get(executionId)).orElse(new HashMap<>());
        if (!withSubContext) {
            return context.entrySet().stream()
                    .filter(entry -> !entry.getKey().startsWith(ReservedConstant.SUB_CONTEXT_PREFIX))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return context;
    }

    @Override
    public void clearContext(String executionId) {
        contextCache.remove(executionId);
    }

    @Override
    public void clearDAGInfo(String executionId) {
        dagInfoCache.remove(executionId);
    }

    @Override
    public void clearDAGInfo(String executionId, int expireTimeInSecond) {
        dagInfoCache.remove(executionId);
    }

    @Override
    public DAG getDAGDescriptor(String executionId) {
        return dagInfoCache.get(executionId).getDag();
    }

    @Override
    public void updateDAGDescriptor(String executionId, DAG dag) {
        dagInfoCache.get(executionId).setDag(dag);
    }
}
