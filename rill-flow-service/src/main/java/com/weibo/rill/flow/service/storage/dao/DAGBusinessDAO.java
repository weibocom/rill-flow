package com.weibo.rill.flow.service.storage.dao;

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
public class DAGBusinessDAO {

    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;

    public boolean createBusiness(String businessId) {
        if (DAGDescriptorUtil.nameInvalid(businessId)) {
            log.info("createBusiness params invalid, businessId:{}", businessId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.sadd(DAGDescriptorUtil.BUSINESS_ID, businessId);
        return true;
    }

    public boolean remBusiness(String businessId) {
        if (DAGDescriptorUtil.nameInvalid(businessId)) {
            log.info("remBusiness params invalid, businessId:{}", businessId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(DAGDescriptorUtil.BUSINESS_ID, businessId);
        return true;
    }

    public Set<String> getBusiness() {
        return redisClient.smembers(DAGDescriptorUtil.BUSINESS_ID, DAGDescriptorUtil.BUSINESS_ID);
    }
}
