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

package com.weibo.api.flow.executors.configuration;

import com.weibo.api.flow.executors.redis.BeanConfig;
import com.weibo.api.flow.executors.redis.RedisClient;
import com.weibo.api.flow.executors.redis.RedisClientGeneratorImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

@Configuration
public class RedisConfiguration {

    @Value("${rill_flow_default_redis_port}")
    private String rillFlowDefaultRedisPort;

    @Value("${rill_flow_default_redis_host}")
    private String rillFlowDefaultRedisServer;

    @Resource
    private RedisClientGeneratorImpl redisClientGeneratorImpl;

    @Bean("descriptorRedisClient")
    public RedisClient descriptorRedisClient() {
        BeanConfig beanConfig = new BeanConfig();
        BeanConfig.Redis redis = new BeanConfig.Redis();
        redis.setMaster(rillFlowDefaultRedisServer);
        redis.setPort(rillFlowDefaultRedisPort);
        beanConfig.setRedis(redis);
        return redisClientGeneratorImpl.newInstance(beanConfig);
    }
}
