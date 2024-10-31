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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGStorageKeysUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Component
@Slf4j
public class DAGABTestDAO {
    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;

    public void createABConfigKey(String businessId, String configKey) {
        if (DAGStorageKeysUtil.nameInvalid(businessId, configKey)) {
            log.info("createABConfigKey params invalid, businessId:{}, configKey:{}", businessId, configKey);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }
        redisClient.sadd(businessId, DAGStorageKeysUtil.buildABConfigKeyRedisKey(businessId), Lists.newArrayList(configKey));
    }

    public Set<String> getABConfigKey(String businessId) {
        return redisClient.smembers(businessId, DAGStorageKeysUtil.buildABConfigKeyRedisKey(businessId));
    }

    public boolean createFunctionAB(String businessId, String configKey, String resourceName, String abRule) {
        if (DAGStorageKeysUtil.nameInvalid(businessId, configKey) || DAGStorageKeysUtil.containsEmpty(resourceName, abRule)) {
            log.info("createFunctionAB param invalid, businessId:{}, configKey:{}, resourceName:{}, abRule:{}", businessId, configKey, resourceName, abRule);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        createABConfigKey(businessId, configKey);

        if (!DAGStorageKeysUtil.DEFAULT.equals(abRule) && StringUtils.isEmpty(getFunctionAB(businessId, configKey).getLeft())) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "default resource value should be configured");
        }
        String resourceNameStorage = DAGStorageKeysUtil.DEFAULT.equals(abRule) ? DAGStorageKeysUtil.DEFAULT + resourceName : resourceName;
        redisClient.hmset(businessId, DAGStorageKeysUtil.buildFunctionABRedisKey(businessId, configKey), ImmutableMap.of(resourceNameStorage, abRule));
        return true;
    }

    public boolean remFunctionAB(String businessId, String configKey, String resourceName) {
        if (DAGStorageKeysUtil.nameInvalid(businessId, configKey) || DAGStorageKeysUtil.containsEmpty(resourceName)) {
            log.info("remFunctionAB params invalid, businessId:{}, configKey:{}, resourceName:{}", businessId, configKey, resourceName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.hdel(businessId, DAGStorageKeysUtil.buildFunctionABRedisKey(businessId, configKey), Lists.newArrayList(resourceName));
        return true;
    }

    public Pair<String, Map<String, String>> getFunctionAB(String businessId, String configKey) {
        Map<String, String> redisRet = redisClient.hgetAll(businessId, DAGStorageKeysUtil.buildFunctionABRedisKey(businessId, configKey));

        String defaultResourceName = null;
        Map<String, String> resourceNameToABRules = Maps.newHashMap();
        for (Map.Entry<String, String> resourceToRule : redisRet.entrySet()) {
            String resourceName = resourceToRule.getKey();
            String rule = resourceToRule.getValue();
            if (StringUtils.isNotEmpty(rule) && rule.equals(DAGStorageKeysUtil.DEFAULT)) {
                defaultResourceName = resourceName.replaceFirst(DAGStorageKeysUtil.DEFAULT, StringUtils.EMPTY);
            } else {
                resourceNameToABRules.put(resourceName, rule);
            }
        }

        return Pair.of(defaultResourceName, resourceNameToABRules);
    }
}
