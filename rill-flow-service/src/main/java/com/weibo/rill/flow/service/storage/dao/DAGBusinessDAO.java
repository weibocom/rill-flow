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

import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGStorageKeysUtil;
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
        if (DAGStorageKeysUtil.nameInvalid(businessId)) {
            log.info("createBusiness params invalid, businessId:{}", businessId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.sadd(DAGStorageKeysUtil.BUSINESS_ID, businessId);
        return true;
    }

    public boolean remBusiness(String businessId) {
        if (DAGStorageKeysUtil.nameInvalid(businessId)) {
            log.info("remBusiness params invalid, businessId:{}", businessId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(DAGStorageKeysUtil.BUSINESS_ID, businessId);
        return true;
    }

    public Set<String> getBusiness() {
        return redisClient.smembers(DAGStorageKeysUtil.BUSINESS_ID, DAGStorageKeysUtil.BUSINESS_ID);
    }
}
