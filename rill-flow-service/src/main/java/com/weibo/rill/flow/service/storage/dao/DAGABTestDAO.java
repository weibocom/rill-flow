package com.weibo.rill.flow.service.storage.dao;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGDescriptorUtil;
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
        if (DAGDescriptorUtil.nameInvalid(businessId, configKey)) {
            log.info("createABConfigKey params invalid, businessId:{}, configKey:{}", businessId, configKey);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }
        redisClient.sadd(businessId, DAGDescriptorUtil.buildABConfigKeyRedisKey(businessId), Lists.newArrayList(configKey));
    }

    public Set<String> getABConfigKey(String businessId) {
        return redisClient.smembers(businessId, DAGDescriptorUtil.buildABConfigKeyRedisKey(businessId));
    }

    public boolean createFunctionAB(String businessId, String configKey, String resourceName, String abRule) {
        if (DAGDescriptorUtil.nameInvalid(businessId, configKey) || DAGDescriptorUtil.containsEmpty(resourceName, abRule)) {
            log.info("createFunctionAB param invalid, businessId:{}, configKey:{}, resourceName:{}, abRule:{}", businessId, configKey, resourceName, abRule);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        createABConfigKey(businessId, configKey);

        if (!DAGDescriptorUtil.DEFAULT.equals(abRule) && StringUtils.isEmpty(getFunctionAB(businessId, configKey).getLeft())) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "default resource value should be configured");
        }
        String resourceNameStorage = DAGDescriptorUtil.DEFAULT.equals(abRule) ? DAGDescriptorUtil.DEFAULT + resourceName : resourceName;
        redisClient.hmset(businessId, DAGDescriptorUtil.buildFunctionABRedisKey(businessId, configKey), ImmutableMap.of(resourceNameStorage, abRule));
        return true;
    }

    public boolean remFunctionAB(String businessId, String configKey, String resourceName) {
        if (DAGDescriptorUtil.nameInvalid(businessId, configKey) || DAGDescriptorUtil.containsEmpty(resourceName)) {
            log.info("remFunctionAB params invalid, businessId:{}, configKey:{}, resourceName:{}", businessId, configKey, resourceName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.hdel(businessId, DAGDescriptorUtil.buildFunctionABRedisKey(businessId, configKey), Lists.newArrayList(resourceName));
        return true;
    }

    public Pair<String, Map<String, String>> getFunctionAB(String businessId, String configKey) {
        Map<String, String> redisRet = redisClient.hgetAll(businessId, DAGDescriptorUtil.buildFunctionABRedisKey(businessId, configKey));

        String defaultResourceName = null;
        Map<String, String> resourceNameToABRules = Maps.newHashMap();
        for (Map.Entry<String, String> resourceToRule : redisRet.entrySet()) {
            String resourceName = resourceToRule.getKey();
            String rule = resourceToRule.getValue();
            if (StringUtils.isNotEmpty(rule) && rule.equals(DAGDescriptorUtil.DEFAULT)) {
                defaultResourceName = resourceName.replaceFirst(DAGDescriptorUtil.DEFAULT, StringUtils.EMPTY);
            } else {
                resourceNameToABRules.put(resourceName, rule);
            }
        }

        return Pair.of(defaultResourceName, resourceNameToABRules);
    }
}
