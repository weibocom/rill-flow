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

import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public class BusinessTimeChecker extends DefaultTimeChecker {
    @Value("${weibo.flow.time.check.all.redis.key:all_time_check_redis_key}")
    private String timeCheckKey;

    public BusinessTimeChecker(RedisClient redisClient) {
        setRedisClient(redisClient);
    }

    @Override
    protected String timeCheckKey() {
        log.info("key:{}", timeCheckKey);
        return timeCheckKey;
    }

    @Override
    protected String buildTimeCheckRedisKey(String executionId) {
        return ExecutionIdUtil.changeExecutionIdToFixedSuffix(executionId, "time_check");
    }

    @Override
    public void initCheckThread(int memberCheckPeriodInSeconds) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleWithFixedDelay(
                this::timeCheckWithRequestId,
                memberCheckPeriodInSeconds, memberCheckPeriodInSeconds, TimeUnit.SECONDS);
    }

    public void timeCheckWithRequestId() {
        try {
            MDC.put("request_id", UUID.randomUUID().toString());
            timeCheck();
        } catch (Exception e) {
            log.warn("asyncExecution execute fails, ", e);
        }
    }
}
