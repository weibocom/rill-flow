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

package com.weibo.rill.flow.service.statistic;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.service.storage.RuntimeRedisClients;
import com.weibo.rill.flow.olympicene.storage.redis.lock.ResourceLoader;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


@Slf4j
@Service
public class TrafficRateLimiter {
    private static final String RATE_KEY_FORMAT = "rate_%s_%s";
    private static final String ACQUIRE_PERMISSION;

    static {
        try {
            ACQUIRE_PERMISSION = ResourceLoader.loadResourceAsText("lua/traffic_rate_limit.lua");
        } catch (IOException e) {
            throw new TaskException(BizError.ERROR_INTERNAL, "cannot load traffic_rate_limit.lua", e.getCause());
        }
    }

    @Autowired
    @Qualifier("runtimeRedisClients")
    private RuntimeRedisClients runtimeRedisClients;

    /**
     * @return true: 不限速 false otherwise
     */
    public boolean tryAcquire(String executionId, String id, int maxRate) {
        try {
            List<String> keys = Lists.newArrayList(buildRateKey(id));
            List<String> args = Lists.newArrayList(String.valueOf(maxRate));
            return (Long) runtimeRedisClients.choose(executionId).eval(ACQUIRE_PERMISSION, keys, args) == 1L;
        } catch (Exception e) {
            log.warn("tryAcquire fails, executionId:{}, id:{}, maxRate:{}", executionId, id, maxRate, e);
            return true;
        }
    }

    private String buildRateKey(String id) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = dateFormat.format(new Date());
        return String.format(RATE_KEY_FORMAT, id, time);
    }
}
