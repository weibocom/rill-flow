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

package com.weibo.rill.flow.executors.configuration;

import com.weibo.rill.flow.executors.model.BeanConfig;
import com.weibo.rill.flow.executors.redis.RedisClient;
import com.weibo.rill.flow.executors.redis.RedisClientGenerator;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfiguration {
    @Value("${sse_executor_redis_host}")
    private String sseExecutorRedisHost;

    @Value("${sse_executor_redis_port}")
    private String sseExecutorRedisPort;

    private static final String SSE_EXECUTOR_REDIS_HOST_ENV = System.getenv("SSE_EXECUTOR_REDIS_HOST");
    private static final String SSE_EXECUTOR_REDIS_PORT_ENV = System.getenv("SSE_EXECUTOR_REDIS_PORT");

    @Autowired
    private RedisClientGenerator<RedisClient> redisClientGeneratorImpl;

    @Bean("sseExecutorRedisClient")
    public RedisClient descriptorRedisClient() {
        String host = sseExecutorRedisHost;
        String port = sseExecutorRedisPort;
        if (StringUtils.isNotEmpty(SSE_EXECUTOR_REDIS_HOST_ENV)) {
            host = SSE_EXECUTOR_REDIS_HOST_ENV;
        }
        if (StringUtils.isNotEmpty(SSE_EXECUTOR_REDIS_PORT_ENV)) {
            port = SSE_EXECUTOR_REDIS_PORT_ENV;
        }

        BeanConfig beanConfig = new BeanConfig();
        BeanConfig.Redis redis = new BeanConfig.Redis();
        redis.setMaster(host);
        redis.setPort(port);
        beanConfig.setRedis(redis);
        return redisClientGeneratorImpl.newInstance(beanConfig);
    }
}
