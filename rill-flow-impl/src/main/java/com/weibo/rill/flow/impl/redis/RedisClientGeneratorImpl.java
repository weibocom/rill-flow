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
import org.springframework.stereotype.Component;


@Component("redisClientGeneratorImpl")
public class RedisClientGeneratorImpl implements RedisClientGenerator<RedisClient> {

    @Override
    public boolean accept(Class<?> targetBeanType) {
        if (targetBeanType == null) {
            return false;
        }
        return RedisClient.class.equals(targetBeanType) || targetBeanType.isAssignableFrom(RedisClient.class);
    }

    @Override
    public RedisClient newInstance(BeanConfig beanConfig) {
        return new JedisFlowClient(beanConfig.getRedis().getMaster(), Integer.parseInt(beanConfig.getRedis().getPort()));
    }
}
