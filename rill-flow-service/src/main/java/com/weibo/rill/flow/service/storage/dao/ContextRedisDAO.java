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

import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.service.util.ValueExtractor;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.save.impl.ContextDAO;


public class ContextRedisDAO extends ContextDAO {

    private final BizDConfs bizDConfs;
    private final SwitcherManager switcherManagerImpl;

    public ContextRedisDAO(RedisClient redisClient, BizDConfs bizDConfs, SwitcherManager switcherManagerImpl) {
        super(redisClient);
        this.bizDConfs = bizDConfs;
        this.switcherManagerImpl = switcherManagerImpl;
    }

    @Override
    protected int getFinishStatusReserveTimeInSecond(String executionId) {
        return ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToFinishReserveSecond(), 86400);
    }

    @Override
    protected int getUnfinishedStatusReserveTimeInSecond(String executionId) {
        return ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToUnfinishedReserveSecond(), 259200);
    }

    @Override
    protected boolean enableContextLengthCheck(String executionId) {
        return switcherManagerImpl.getSwitcherState("ENABLE_DAG_CONTEXT_LENGTH_CHECK");
    }

    @Override
    protected int contextMaxLength(String executionId) {
        return ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToContextMaxLength(), 36 * 1024);
    }
}
