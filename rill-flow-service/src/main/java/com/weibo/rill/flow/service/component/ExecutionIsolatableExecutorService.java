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

package com.weibo.rill.flow.service.component;

import com.weibo.rill.flow.service.util.UuidUtil;
import com.weibo.rill.flow.olympicene.storage.redis.api.Crc32Sharding;
import com.weibo.rill.flow.olympicene.storage.redis.api.Sharding;
import com.weibo.rill.flow.olympicene.core.concurrent.ExecutionRunnable;
import com.weibo.rill.flow.common.concurrent.BaseExecutorService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;

@Slf4j
public class ExecutionIsolatableExecutorService extends BaseExecutorService {

    private List<ExecutorService> executors;
    private Sharding<ExecutorService> sharding;

    public ExecutionIsolatableExecutorService(List<ExecutorService> executors) {
        this.executors = executors;
        this.sharding = Crc32Sharding.singleton();
    }
    public ExecutionIsolatableExecutorService(List<ExecutorService> executors, Sharding<ExecutorService> sharding) {
        this.executors = executors;
        this.sharding = sharding;
    }

    @Override
    public void execute(Runnable runnable) {
        choose(getShardingKey(runnable), this.executors).execute(runnable);
    }

    public ExecutorService choose(String shardingKey, List<ExecutorService> clients) {
        return sharding.choose(clients, shardingKey);
    }

    public String getShardingKey(Runnable runnable) {
        if (runnable instanceof ExecutionRunnable executionRunnable) {
            return executionRunnable.getExecutionId();
        }

        String key = UuidUtil.uuid().toString();
        log.warn("there no executionId in submitted runnable, use random sharding key:{}.", key);
        return key;
    }

}
