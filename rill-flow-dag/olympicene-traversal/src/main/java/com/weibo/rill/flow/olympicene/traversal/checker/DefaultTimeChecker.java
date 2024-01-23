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

package com.weibo.rill.flow.olympicene.traversal.checker;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.redis.lock.ResourceLoader;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.runners.TimeCheckRunner;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@Slf4j
@NoArgsConstructor
public class DefaultTimeChecker implements TimeChecker {
    private static final String REDIS_GET_TIMEOUT;

    static {
        try {
            REDIS_GET_TIMEOUT = ResourceLoader.loadResourceAsText("lua/redis_get_timeout.lua");
        } catch (IOException e) {
            throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "cannot load redis_get_timeout.lua");
        }
    }

    @Setter
    private RedisClient redisClient;
    @Setter
    private TimeCheckRunner timeCheckRunner;

    public DefaultTimeChecker(int timeoutCheckPeriodInSecond, RedisClient redisClient) {
        this.redisClient = redisClient;
        initCheckThread(timeoutCheckPeriodInSecond);
    }

    // ------------------------------------------------------
    protected String timeCheckKey() {
        return "all_time_check_redis_key";
    }
    // 若executionId能区分不同业务且期望不同业务存不同key 重写该方法
    protected String buildTimeCheckRedisKey(String executionId) {
        log.debug("buildTimeCheckRedisKey executionId:{}", executionId);
        return "time_check";
    }
    // ------------------------------------------------------

    @Override
    public boolean addMemberToCheckPool(String executionId, String member, long time) {
        try {
            log.info("addMemberToCheckPool executionId:{}, member:{}, time:{}", executionId, member, time);

            String key = buildTimeCheckRedisKey(executionId);
            redisClient.zadd(key, time, member);
            redisClient.zadd(timeCheckKey(), System.currentTimeMillis(), key);

            return true;
        } catch (Exception e) {
            log.warn("addMemberToCheckPool fails, executionId:{} member:{} time:{}", executionId, member, time, e);
            return false;
        }
    }

    @Override
    public boolean remMemberFromCheckPool(String executionId, String member) {
        try {
            log.info("remMemberFromCheckPool executionId:{}, member:{}", executionId, member);

            // 不从ALL_TIME_CHECK_REDIS_KEY中删除key
            // 若担心有长时间不更新且值为空的key 可根据score值删除key
            redisClient.zrem(buildTimeCheckRedisKey(executionId), member);

            return true;
        } catch (Exception e) {
            log.warn("recordDAGCompleted fails, executionId:{} member:{}", executionId, member, e);
            return false;
        }
    }

    @Override
    public void initCheckThread(int memberCheckPeriodInSeconds) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(
                this::timeCheck,
                memberCheckPeriodInSeconds, memberCheckPeriodInSeconds, TimeUnit.SECONDS);
    }

    protected void timeCheck() {
        try {
            log.info("timeCheck start");

            Set<String> allKeys = redisClient.zrangeByScore(timeCheckKey(), 0, System.currentTimeMillis());
            log.info("timeCheck keys size:{}", CollectionUtils.isEmpty(allKeys) ? 0 : allKeys.size());
            if (CollectionUtils.isEmpty(allKeys)) {
                return;
            }

            Consumer<String> action = member -> {
                try {
                    timeCheckRunner.handleTimeCheck(member);
                } catch (Exception e) {
                    log.warn("timeCheck fails, member:{}", member, e);
                }
            };
            allKeys.stream().filter(StringUtils::isNotEmpty).forEach(key -> doCheck(key, action));
        } catch (Exception e) {
            log.warn("timeCheck fails, ", e);
        }
    }

    @SuppressWarnings("unchecked")
    protected void doCheck(String redisKey, Consumer<String> action) {
        try {
            log.info("doCheck start redisKey:{}", redisKey);
            List<String> keys = Lists.newArrayList(redisKey);
            List<String> argv = Lists.newArrayList("0", String.valueOf(System.currentTimeMillis()), "0", "30");

            while (true) {
                List<byte[]> membersByte = (List<byte[]>) redisClient.eval(REDIS_GET_TIMEOUT, redisKey, keys, argv);

                List<String> members = Optional.ofNullable(membersByte)
                        .map(it -> it.stream()
                                .filter(Objects::nonNull)
                                .map(member -> new String(member, StandardCharsets.UTF_8))
                                .collect(Collectors.toList()))
                        .orElse(Lists.newArrayList());
                if (CollectionUtils.isEmpty(members)) {
                    break;
                }

                members.forEach(member -> {
                    log.info("doCheck begin to check member:{}", member);
                    action.accept(member);
                });
            }
        } catch (Exception e) {
            log.warn("doCheck fails, redisKey:{}", redisKey, e);
        }
    }
}
