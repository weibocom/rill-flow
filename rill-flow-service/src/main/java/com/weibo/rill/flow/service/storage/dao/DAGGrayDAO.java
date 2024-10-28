package com.weibo.rill.flow.service.storage.dao;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGDescriptorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class DAGGrayDAO {
    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;
    @Autowired
    private DAGAliasDAO dagAliasDAO;

    public boolean createGray(String businessId, String featureName, String alias, String grayRule) {
        if (StringUtils.isEmpty(grayRule) || DAGDescriptorUtil.nameInvalid(businessId, featureName, alias)) {
            log.info("createGray param invalid, businessId:{}, featureName:{}, aliasName:{}, grayRule:{}",
                    businessId, featureName, alias, grayRule);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        dagAliasDAO.createAlias(businessId, featureName, alias);
        redisClient.hmset(businessId, DAGDescriptorUtil.buildGrayRedisKey(businessId, featureName), ImmutableMap.of(alias, grayRule));
        return true;
    }

    public boolean remGray(String businessId, String featureName, String alias) {
        if (DAGDescriptorUtil.nameInvalid(businessId, featureName, alias)) {
            log.info("remGray params invalid, businessId:{}, featureName:{}, alias:{}", businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.hdel(businessId, DAGDescriptorUtil.buildGrayRedisKey(businessId, featureName), Lists.newArrayList(alias));
        return true;
    }

    public Map<String, String> getGray(String businessId, String featureName) {
        return redisClient.hgetAll(businessId, DAGDescriptorUtil.buildGrayRedisKey(businessId, featureName));
    }
}
