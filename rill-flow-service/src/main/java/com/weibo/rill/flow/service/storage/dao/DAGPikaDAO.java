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

package com.weibo.rill.flow.service.storage.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.storage.constant.DAGRedisPrefix;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.save.impl.DagStorageSerializer;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.service.util.ValueExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class DAGPikaDAO {
    private static final String DAG = "dag";

    private final String keyPrefix;
    private final boolean swapStorage;
    private final BizDConfs bizDConfs;
    private final Map<String, RedisClient> clientIdToRedisClient;

    public DAGPikaDAO(boolean swapStorage, Map<String, RedisClient> clientIdToRedisClient, BizDConfs bizDConfs) {
        this.keyPrefix = swapStorage ? "swap_" : "long_";
        this.swapStorage = swapStorage;
        this.bizDConfs = bizDConfs;
        this.clientIdToRedisClient = clientIdToRedisClient;
    }

    public void updateDAGInfo(String executionId, DAGInfo dagInfo) {
        if (dagInfo == null) {
            return;
        }

        log.info("updateDAGInfo executionId:{}", executionId);
        DAGInfo rawDAGInfo = getDAGInfo(executionId);
        DAGInfo dagInfoUpdate;
        if (rawDAGInfo == null) {
            dagInfoUpdate = dagInfo;
        } else {
            rawDAGInfo.update(dagInfo);
            dagInfoUpdate = rawDAGInfo;
        }
        saveDAGInfo(executionId, dagInfoUpdate);
    }

    public void updateTaskInfos(String executionId, Set<TaskInfo> taskInfos) {
        if (CollectionUtils.isEmpty(taskInfos)) {
            log.info("updateTaskInfos taskInfos empty, executionId:{}", executionId);
            return;
        }

        log.info("updateTaskInfos executionId:{}", executionId);
        DAGInfo dagInfo = getDAGInfo(executionId);
        if (dagInfo == null) {
            return;
        }

        taskInfos.forEach(taskInfo -> {
            TaskInfo rawTaskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskInfo.getName());
            if (rawTaskInfo != null) {
                rawTaskInfo.update(taskInfo);
            }
        });
        saveDAGInfo(executionId, dagInfo);
    }

    public void updateDAGDescriptor(String executionId, DAG dag) {
        log.info("updateDAGDescriptor executionId:{}", executionId);
        DAGInfo dagInfo = getDAGInfo(executionId);
        if (dagInfo == null) {
            return;
        }
        dagInfo.setDag(dag);
        saveDAGInfo(executionId, dagInfo);
    }

    public void saveDAGInfo(String executionId, DAGInfo dagInfo) {
        if (dagInfo == null) {
            return;
        }

        ObjectNode dagInfoJson = DAGTraversalSerializer.MAPPER.valueToTree(dagInfo);
        String descriptor = dagInfoJson.get(DAG).toString();
        String dagInfoKey = buildDagInfoKey(executionId);
        String descriptorKey = buildDescriptorKey(descriptor);
        dagInfoJson.put(DAG, descriptorKey);

        RedisClient client = getClient(executionId);
        int expireTime = getUnfinishedReserveTimeInSecond(executionId);
        client.set(dagInfoKey, dagInfoJson.toString());
        client.expire(dagInfoKey, expireTime);
        client.set(descriptorKey, descriptor);
    }

    public void clearDAGInfo(String executionId) {
        log.info("clearDAGInfo executionId:{}", executionId);
        int expireTime = getFinishReserveTimeInSecond(executionId);
        String dagInfoKey = buildDagInfoKey(executionId);
        RedisClient client = getClient(executionId);
        client.expire(dagInfoKey, expireTime);
    }

    public DAGInfo getDAGInfo(String executionId) {
        try {
            log.info("getDAGInfo executionId:{}", executionId);
            String dagInfoKey = buildDagInfoKey(executionId);
            RedisClient client = getClient(executionId);
            String dagInfoRaw = client.get(dagInfoKey);
            if (StringUtils.isBlank(dagInfoRaw)) {
                log.info("getRawDAGInfo empty, dagInfoKey:{}", dagInfoKey);
                return null;
            }

            ObjectNode dagInfoJson = (ObjectNode) DagStorageSerializer.MAPPER.readTree(dagInfoRaw);
            String descriptorKey = dagInfoJson.get(DAG).asText();
            dagInfoJson.set(DAG, DagStorageSerializer.MAPPER.readTree(client.get(descriptorKey)));

            return DagStorageSerializer.MAPPER.convertValue(dagInfoJson, DAGInfo.class);
        } catch (Exception e) {
            log.warn("getDAGInfo fails, executionId:{}", executionId, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(),
                    "can not get dag info, executionId:" + executionId, e.getCause());
        }
    }

    public DAGInfo getBasicDAGInfo(String executionId) {
        DAGInfo dagInfo = getDAGInfo(executionId);
        if (dagInfo != null) {
            Optional.ofNullable(dagInfo.getTasks()).map(Map::values)
                    .ifPresent(taskInfos -> taskInfos.forEach(taskInfo -> taskInfo.setChildren(new LinkedHashMap<>())));
        }
        return dagInfo;
    }

    public TaskInfo getTaskInfo(String executionId, String taskName, String subGroupIndex) {
        DAGInfo dagInfo = getDAGInfo(executionId);

        if (dagInfo == null) {
            return null;
        }

        TaskInfo taskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskName);
        if (taskInfo != null && MapUtils.isNotEmpty(taskInfo.getChildren()) && StringUtils.isNotBlank(subGroupIndex)) {
            String routeName = DAGWalkHelper.getInstance().buildTaskInfoRouteName(taskName, subGroupIndex);
            Map<String, TaskInfo> currentGroupTasks = taskInfo.getChildren().entrySet().stream()
                    .filter(entry -> routeName.equals(entry.getValue().getRouteName()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            taskInfo.setChildren(currentGroupTasks);
        }
        return taskInfo;
    }

    public TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        DAGInfo dagInfo = getDAGInfo(executionId);

        if (dagInfo == null) {
            return null;
        }

        TaskInfo taskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskName);
        if (taskInfo != null) {
            taskInfo.setChildren(new LinkedHashMap<>());
        }
        return taskInfo;
    }

    public void updateContext(String executionId, Map<String, Object> context) {
        if (MapUtils.isEmpty(context)) {
            return;
        }
        log.info("updateContext executionId:{}", executionId);
        Map<String, Object> contextTotal = getTotalContext(executionId);
        contextTotal.putAll(context);
        saveContext(executionId, contextTotal);
    }

    public void saveContext(String executionId, Map<String, Object> context) {
        if (MapUtils.isEmpty(context)) {
            return;
        }
        String contextKey = buildContextKey(executionId);
        RedisClient client = getClient(executionId);
        int expireTime = getUnfinishedReserveTimeInSecond(executionId);
        client.set(contextKey, DagStorageSerializer.serializeToString(context));
        client.expire(contextKey, expireTime);
    }

    public void clearContext(String executionId) {
        log.info("clearContext executionId:{}", executionId);
        int expireTime = getFinishReserveTimeInSecond(executionId);
        String contextKey = buildContextKey(executionId);
        RedisClient client = getClient(executionId);
        client.expire(contextKey, expireTime);
    }

    public Map<String, Object> getTotalContext(String executionId) {
        try {
            String contextKey = buildContextKey(executionId);
            RedisClient client = getClient(executionId);
            String contextRaw = client.get(contextKey);
            if (StringUtils.isBlank(contextRaw)) {
                log.info("getTotalContext empty, contextKey:{}", contextKey);
                return Maps.newHashMap();
            }

            JsonNode jsonNode = DagStorageSerializer.MAPPER.readTree(contextRaw);
            return DagStorageSerializer.MAPPER.convertValue(jsonNode, new TypeReference<>() {
            });
        } catch (Exception e) {
            log.warn("getTotalContext fails, executionId:{}", executionId, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(),
                    "can not get total context, executionId:" + executionId, e.getCause());
        }
    }

    public Map<String, Object> getContext(String executionId, boolean needSubContext) {
        Map<String, Object> contextTotal = getTotalContext(executionId);
        if (MapUtils.isEmpty(contextTotal)) {
            return Collections.emptyMap();
        }

        if (needSubContext) {
            return contextTotal;
        }

        return contextTotal.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(ReservedConstant.SUB_CONTEXT_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, Object> getContext(String executionId, Collection<String> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return Collections.emptyMap();
        }

        Map<String, Object> contextTotal = getTotalContext(executionId);
        if (MapUtils.isEmpty(contextTotal)) {
            return Collections.emptyMap();
        }

        return contextTotal.entrySet().stream()
                .filter(entry -> fields.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private RedisClient getClient(String executionId) {
        String businessId = ExecutionIdUtil.getBusinessId(executionId);
        String clientId = getClientId(businessId);
        log.debug("getClient businessId:{}, clientId:{}", businessId, clientId);

        RedisClient client = clientIdToRedisClient.get(clientId);
        if (client == null) {
            log.warn("getClient clientId:{} not found in config", clientId);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, "client:" + clientId + "not found");
        }
        return client;
    }

    private String getClientId(String businessId) {
        return swapStorage ?
                bizDConfs.getSwapBusinessIdToClientId().get(businessId) : bizDConfs.getLongTermStorageBusinessIdToClientId().get(businessId);
    }

    private int getFinishReserveTimeInSecond(String executionId) {
        Map<String, Integer> timeConfigMap = swapStorage ?
                bizDConfs.getSwapBusinessIdToFinishReserveSecond() : bizDConfs.getLongTermBusinessIdToReserveSecond();
        return ValueExtractor.getConfiguredValue(executionId, timeConfigMap, 259200);
    }

    private int getUnfinishedReserveTimeInSecond(String executionId) {
        Map<String, Integer> timeConfigMap = swapStorage ?
                bizDConfs.getSwapBusinessIdToUnfinishedReserveSecond() : bizDConfs.getLongTermBusinessIdToReserveSecond();
        return ValueExtractor.getConfiguredValue(executionId, timeConfigMap, 259200);
    }

    private String buildDagInfoKey(String executionId) {
        return keyPrefix + DAGRedisPrefix.PREFIX_DAG_INFO.getValue() + executionId;
    }

    private String buildDescriptorKey(String descriptor) {
        String md5 = DigestUtils.md5Hex(descriptor);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMM");
        String time = dateFormat.format(new Date());
        return keyPrefix + DAGRedisPrefix.PREFIX_DAG_DESCRIPTOR.getValue() + time + "_" + md5;
    }

    private String buildContextKey(String executionId) {
        return keyPrefix + DAGRedisPrefix.PREFIX_CONTEXT.getValue() + executionId;
    }
}
