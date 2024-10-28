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
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGDescriptorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DAGAliasDAO {
    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;
    @Autowired
    private DAGFeatureDAO dagFeatureDAO;

    public boolean createAlias(String businessId, String featureName, String alias) {
        if (DAGDescriptorUtil.nameInvalid(businessId, featureName, alias)) {
            log.info("createAlias params invalid, businessId:{}, featureName:{}, alias:{}",
                    businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        dagFeatureDAO.createFeature(businessId, featureName);
        redisClient.sadd(businessId, DAGDescriptorUtil.buildAliasRedisKey(businessId, featureName), Lists.newArrayList(alias));
        return true;
    }

    public boolean remAlias(String businessId, String featureName, String alias) {
        if (DAGDescriptorUtil.nameInvalid(businessId, featureName, alias)) {
            log.info("remAlias params invalid, businessId:{}, featureName:{}, alias:{}", businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(businessId, DAGDescriptorUtil.buildAliasRedisKey(businessId, featureName), Lists.newArrayList(alias));
        return true;
    }

    public Set<String> getAlias(String businessId, String featureName) {
        return redisClient.smembers(businessId, DAGDescriptorUtil.buildAliasRedisKey(businessId, featureName));
    }

    public String getDescriptorRedisKeyByAlias(String businessId, String featureName, String alias) {
        if (DAGDescriptorUtil.nameInvalid(businessId, featureName, alias)) {
            log.info("getDescriptorRedisKeyByAlias param invalid, businessId:{}, featureName:{}, alias:{}", businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        Set<String> redisRet = redisClient.zrange(businessId, DAGDescriptorUtil.buildVersionRedisKey(businessId, featureName, alias), -1, -1);
        if (CollectionUtils.isEmpty(redisRet)) {
            log.info("getDescriptorRedisKeyByAlias redisRet empty");
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("cannot find descriptor: %s:%s:%s", businessId, featureName, alias));
        }

        String md5 = redisRet.iterator().next();
        log.info("getDescriptorRedisKeyByAlias md5:{}", md5);
        return DAGDescriptorUtil.buildDescriptorRedisKey(businessId, featureName, md5);
    }

    public List<Map> getVersion(String businessId, String featureName, String alias) {
        Set<Pair<String, Double>> redisRet = redisClient.zrangeWithScores(businessId, DAGDescriptorUtil.buildVersionRedisKey(businessId, featureName, alias), 0, -1);
        return redisRet.stream()
                .map(memberToScore -> {
                    String md5 = memberToScore.getKey();
                    Long createTime = memberToScore.getValue().longValue();
                    return Pair.of(DAGDescriptorUtil.buildDescriptorId(businessId, featureName, DAGDescriptorUtil.MD5_PREFIX + md5), createTime);
                })
                .sorted((c1, c2) -> c2.getValue().compareTo(c1.getValue()))
                .map(idToCreateTime -> ImmutableMap.of("descriptor_id", idToCreateTime.getLeft(), "create_time", idToCreateTime.getRight()))
                .collect(Collectors.toList());
    }
}
