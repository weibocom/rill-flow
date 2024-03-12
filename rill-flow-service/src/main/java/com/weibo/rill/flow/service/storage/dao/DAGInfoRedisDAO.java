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

package com.weibo.rill.flow.service.storage.dao;

import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDAO;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDeserializeService;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker;
import com.weibo.rill.flow.service.util.ValueExtractor;

import java.util.List;


public class DAGInfoRedisDAO extends DAGInfoDAO {
    private final BizDConfs bizDConfs;
    private final DAGSubmitChecker dagSubmitChecker;

    public DAGInfoRedisDAO(RedisClient redisClient, BizDConfs bizDConfs,
                           DAGInfoDeserializeService dagInfoDeserializeService, DAGSubmitChecker dagSubmitChecker) {
        super(redisClient, dagInfoDeserializeService);
        this.bizDConfs = bizDConfs;
        this.dagSubmitChecker = dagSubmitChecker;
    }

    @Override
    public int getFinishStatusReserveTimeInSecond(String executionId) {
        return ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToFinishReserveSecond(), 86400);
    }

    @Override
    protected int getUnfinishedStatusReserveTimeInSecond(String executionId) {
        return ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToUnfinishedReserveSecond(), 259200);
    }

    @Override
    protected void checkDAGInfoLength(String executionId, List<byte[]> contents) {
        dagSubmitChecker.checkDAGInfoLength(executionId, contents);
    }
}
