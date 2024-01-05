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

package com.weibo.rill.flow.impl.redis;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.manager.DAGClientPool;
import com.weibo.rill.flow.service.storage.CustomizedStorage;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.service.util.ValueExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class CustomizedStorageImpl implements CustomizedStorage {
    @Autowired
    private BizDConfs bizDConfs;
    @Autowired
    private DAGClientPool dagClientPool;

    public String initBucket(String bucketName, JSONObject fieldToValues) {
        long currentTimeSecond = Instant.now().getEpochSecond();
        int reserveTimeInSecond = getReserveTimeInSecond(bucketName);

        JedisFlowClient jedisFlowClient = getJedisClient(bucketName);
        jedisFlowClient.pipelined().accept(pipeline -> {
            pipeline.hset(bucketName, "expire", String.valueOf(currentTimeSecond + reserveTimeInSecond));
            if (MapUtils.isNotEmpty(fieldToValues)) {
                fieldToValues.forEach((field, value) -> pipeline.hset(bucketName, field, value.toString()));
            }
            pipeline.expire(bucketName, reserveTimeInSecond);
            pipeline.sync();
        });
        return bucketName;
    }

    public void store(String bucketName, JSONObject fieldToValues) {
        if (MapUtils.isEmpty(fieldToValues)) {
            return;
        }

        JedisFlowClient jedisFlowClient = getJedisClient(bucketName);
        jedisFlowClient.pipelined().accept(pipeline -> {
            fieldToValues.forEach((field, value) -> pipeline.hset(bucketName, field, value.toString()));
            pipeline.sync();
        });
    }

    public Map<String, String> load(String bucketName, boolean hGetAll, List<String> fieldNames, String fieldPrefix) {
        JedisFlowClient jedisFlowClient = getJedisClient(bucketName);
        List<String> fields = jedisFlowClient.hkeys(bucketName).stream()
                .filter(fieldName ->
                        (CollectionUtils.isNotEmpty(fieldNames) && fieldNames.contains(fieldName)) ||
                                (StringUtils.isNotBlank(fieldPrefix) && fieldName.startsWith(fieldPrefix)))
                .toList();

        if (CollectionUtils.isEmpty(fields)) {
            return Collections.emptyMap();
        }
        List<String> values = jedisFlowClient.hmget(bucketName, fields.toArray(new String[0]));
        long nullValueCount = values.stream().filter(Objects::isNull).count();
        if (nullValueCount > 0) {
            values = jedisFlowClient.hmget(bucketName, fields.toArray(new String[0]));
        }
        Map<String, String> ret = Maps.newHashMap();
        for (int i = 0; i < fields.size(); i++) {
            ret.put(fields.get(i), values.get(i));
        }
        return ret;
    }

    public boolean remove(String bucketName) {
        JedisFlowClient jedisFlowClient = getJedisClient(bucketName);
        jedisFlowClient.del(bucketName);
        return true;
    }

    public boolean remove(String bucketName, List<String> fieldNames) {
        JedisFlowClient jedisFlowClient = getJedisClient(bucketName);
        if (CollectionUtils.isNotEmpty(fieldNames)) {
            jedisFlowClient.pipelined().accept(pipeline -> fieldNames.forEach(fieldName -> pipeline.hdel(bucketName, fieldName)));
        }
        return true;
    }

    private int getReserveTimeInSecond(String bucketName) {
        return ValueExtractor.getConfiguredValue(bucketName, bizDConfs.getCustomizedBusinessIdToReserveSecond(), 86400);
    }

    private JedisFlowClient getJedisClient(String bucketName) {
        String serviceId = ExecutionIdUtil.getServiceId(bucketName);
        String clientId = bizDConfs.getCustomizedServiceIdToClientId().get(serviceId);
        if (StringUtils.isBlank(clientId)) {
            String businessId = ExecutionIdUtil.getBusinessId(bucketName);
            clientId = bizDConfs.getCustomizedBusinessIdToClientId().get(businessId);
        }
        log.debug("getClient bucketName:{}, clientId:{}", bucketName, clientId);

        Map<String, RedisClient> clientMap = dagClientPool.getCustomizedStorageClientIdToRedisClient();
        if (StringUtils.isBlank(clientId) || !clientMap.containsKey(clientId)) {
            log.warn("clientId:{} not found in config", clientId);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, "client not configured");
        }

        RedisClient client = dagClientPool.getCustomizedStorageClientIdToRedisClient().get(clientId);
        if (!(client instanceof JedisFlowClient)) {
            log.warn("redisClient type is not support clientId:{}", clientId);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, "client:" + clientId + " type nonsupport");
        }
        return (JedisFlowClient) client;
    }
}
