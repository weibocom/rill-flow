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

package com.weibo.rill.flow.service.storage;

import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.redis.apicommons.GroupedRedisClient;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;


@Slf4j
public class RuntimeRedisClients extends GroupedRedisClient {
    private final BizDConfs bizDConfs;
    private final Map<String, RedisClient> clientIdToRedisClient;
    private final RedisClient defaultRedisClient;

    public RuntimeRedisClients(BizDConfs bizDConfs, Map<String, RedisClient> clientIdToRedisClient, RedisClient defaultRedisClient) {
        this.bizDConfs = bizDConfs;
        this.clientIdToRedisClient = clientIdToRedisClient;
        this.defaultRedisClient = defaultRedisClient;
    }

    @Override
    public RedisClient choose(String shardingKey) {
        String serviceId = ExecutionIdUtil.getServiceId(shardingKey);
        String clientId = bizDConfs.getRedisServiceIdToClientId().get(serviceId);
        if (StringUtils.isBlank(clientId)) {
            String businessId = ExecutionIdUtil.getBusinessId(shardingKey);
            clientId = bizDConfs.getRedisBusinessIdToClientId().get(businessId);
        }
        log.debug("choose shardingKey:{}, clientId:{}", shardingKey, clientId);

        if (StringUtils.isBlank(clientId)) {
            return defaultRedisClient;
        }

        RedisClient client = clientIdToRedisClient.get(clientId);
        if (client == null) {
            log.warn("choose clientId:{} not found in config", clientId);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, "client:" + clientId + "not found");
        }
        return client;
    }
}
