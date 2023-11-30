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

import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LocalStorageProcedure implements DAGStorageProcedure {
    private final Map<String, ReadWriteLock> lockers = new ConcurrentHashMap<>();

    @Override
    public void lockAndRun(String lockName, Runnable runnable) {
        Lock lock = null;
        try {
            ReadWriteLock dagInfoLock = lockers.computeIfAbsent(lockName, it -> new ReentrantReadWriteLock());
            lock = dagInfoLock.writeLock();
            if (tryLock(lockName, lock)) {
                log.info("get lock lockName:{}", lockName);
                runnable.run();
            } else {
                log.warn("cannot get lock:{}", lockName);
                throw new StorageException(StorageErrorCode.LOCK_TIMEOUT.getCode(), "lock fails");
            }
        } finally {
            lockers.remove(lockName);
            if (lock != null) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    log.warn("unlock {} failed. ", lockName, e);
                }
            }
        }
    }

    private boolean tryLock(String lockName, Lock lock) {
        try {
            return lock.tryLock(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("tryLock fails, lockName:{}", lockName, e);
            return false;
        }
    }
}
