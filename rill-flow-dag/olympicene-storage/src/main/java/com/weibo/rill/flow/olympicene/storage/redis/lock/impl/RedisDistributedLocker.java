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

package com.weibo.rill.flow.olympicene.storage.redis.lock.impl;

import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.redis.lock.Locker;
import com.weibo.rill.flow.olympicene.storage.redis.lock.ResourceLoader;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class RedisDistributedLocker implements Locker {
    private static final String REDIS_LOCK;
    private static final String REDIS_UNLOCK;
    static {
        try {
            REDIS_LOCK = ResourceLoader.loadResourceAsText("lua/redis_lock.lua");
            REDIS_UNLOCK = ResourceLoader.loadResourceAsText("lua/redis_unlock.lua");
        } catch (IOException e) {
            throw new RuntimeException("load script fails", e.getCause());
        }
    }
    private final RedisClient redisClient;

    @Setter
    private long lockTimeout = 10 * 1000L;

    public RedisDistributedLocker(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public void lock(String lockName, String lockAcquirerId, long expire) {
        long startTime = System.currentTimeMillis();
        int count = 0;
        while (true) {
            count++;
            Object redisLockObject = evalScript(REDIS_LOCK, List.of(lockName), List.of(lockAcquirerId, String.valueOf(expire)));
            String ret = redisLockObject instanceof String? String.valueOf(redisLockObject): new String((byte[]) redisLockObject);
            if (Objects.equals(ret, "OK")) {
                break;
            }
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > lockTimeout) {
                throw new RuntimeException("try " + count + " times lock " + lockName + " timeout " + elapsed);
            }
            if (count % 10 == 0) {
                log.debug("lock {} value {} failed for {} times, cost {} ms",
                        lockName, lockAcquirerId, count, System.currentTimeMillis() - startTime);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        log.debug("lock {} value {} success in {} times, cost {} ms",
                lockName, lockAcquirerId, count, System.currentTimeMillis() - startTime);
    }

    @Override
    public void unlock(String lockName, String lockAcquirerId) {
        Object ret = evalScript(REDIS_UNLOCK, List.of(lockName), List.of(lockAcquirerId));
        log.debug("unlock {} value {}, result {}", lockName, lockAcquirerId, ret);
    }

    private Object evalScript(String script, List<String> keys, List<String> values) {
        return redisClient.eval(script, keys, values);
    }
}
