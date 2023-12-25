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

package com.weibo.rill.flow.olympicene.storage.save.impl;

import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.storage.redis.lock.Locker;
import lombok.Setter;


public class RedisStorageProcedure implements DAGStorageProcedure {
    private final String instanceId;
    private final Locker locker;

    @Setter
    private int lockExpireTimeInSecond = 300;

    public RedisStorageProcedure(String instanceId, Locker locker) {
        this.instanceId = instanceId;
        this.locker = locker;
    }

    @Override
    public void lockAndRun(String lockName, Runnable runnable) {
        String lockAcquirerId = LockerKey.getLockId(instanceId);
        try {
            locker.lock(lockName, lockAcquirerId, lockExpireTimeInSecond);
            runnable.run();
        } finally {
            locker.unlock(lockName, lockAcquirerId);
        }
    }
}
