package com.weibo.rill.flow.service.storage.dao;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGDescriptorUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@Slf4j
public class DAGFeatureDAO {
    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;
    @Autowired
    private DAGBusinessDAO dagBusinessDAO;

    public boolean createFeature(String businessId, String featureName) {
        if (DAGDescriptorUtil.nameInvalid(businessId, featureName)) {
            log.info("createFeature params invalid, businessId:{}, serviceName:{}", businessId, featureName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        dagBusinessDAO.createBusiness(businessId);
        redisClient.sadd(businessId, DAGDescriptorUtil.buildFeatureRedisKey(businessId), Lists.newArrayList(featureName));
        return true;
    }

    public boolean remFeature(String businessId, String featureName) {
        if (DAGDescriptorUtil.nameInvalid(businessId, featureName)) {
            log.info("remFeature params invalid, businessId:{}, featureName:{}", businessId, featureName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(businessId, DAGDescriptorUtil.buildFeatureRedisKey(businessId), Lists.newArrayList(featureName));
        return true;
    }

    public Set<String> getFeature(String businessId) {
        return redisClient.smembers(businessId, DAGDescriptorUtil.buildFeatureRedisKey(businessId));
    }
}
