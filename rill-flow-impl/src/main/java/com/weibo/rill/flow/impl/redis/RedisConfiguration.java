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

import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.configuration.BeanConfig;
import com.weibo.rill.flow.service.configuration.RedisClientGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfiguration {
    @Value("${rill_flow_descriptor_redis_host}")
    private String rillFlowDescriptorServer;

    @Value("${rill_flow_descriptor_redis_port}")
    private String rillFlowDescriptorPort;

    @Value("${rill_flow_default_redis_port}")
    private String rillFlowDagStoragePort;

    @Value("${rill_flow_default_redis_host}")
    private String rillFlowDagStorageServer;

    @Autowired
    private RedisClientGenerator<RedisClient> redisClientGeneratorImpl;

    @Bean("descriptorRedisClient")
    public RedisClient descriptorRedisClient() {
        BeanConfig beanConfig = new BeanConfig();
        BeanConfig.Redis redis = new BeanConfig.Redis();
        redis.setMaster(rillFlowDescriptorServer);
        redis.setPort(rillFlowDescriptorPort);
        beanConfig.setRedis(redis);
        return redisClientGeneratorImpl.newInstance(beanConfig);
    }

    @Bean("dagDefaultStorageRedisClient")
    public RedisClient dagDefaultStorageRedisClient() {
        BeanConfig beanConfig = new BeanConfig();
        BeanConfig.Redis redis = new BeanConfig.Redis();
        redis.setMaster(rillFlowDagStorageServer);
        redis.setPort(rillFlowDagStoragePort);
        beanConfig.setRedis(redis);
        return redisClientGeneratorImpl.newInstance(beanConfig);
    }
}
