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

import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.exception.StorageException;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDAO;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDeserializeService;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.util.ValueExtractor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;


public class DAGInfoRedisDAO extends DAGInfoDAO {
    private SwitcherManager switcherManagerImpl;

    private static final int DAG_INFO_MAX_LENGTH_CONFIG = 30 * 1024 + 600 * 1024; // dag描述符最大为30K 每个任务大小最大为600B最大存1000个任务

    private final BizDConfs bizDConfs;

    public DAGInfoRedisDAO(RedisClient redisClient, BizDConfs bizDConfs,
                           DAGInfoDeserializeService dagInfoDeserializeService, SwitcherManager switcherManagerImpl) {
        super(redisClient, dagInfoDeserializeService);
        this.bizDConfs = bizDConfs;
        this.switcherManagerImpl = switcherManagerImpl;
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
        if (!switcherManagerImpl.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") || CollectionUtils.isEmpty(contents)) {
            return;
        }

        int length = contents.stream().filter(Objects::nonNull).mapToInt(content -> content.length).sum();
        int maxLength = ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToDAGInfoMaxLength(), DAG_INFO_MAX_LENGTH_CONFIG);
        if (length > maxLength) {
            throw new StorageException(
                    StorageErrorCode.DAG_LENGTH_LIMITATION.getCode(),
                    String.format("dag info length:%s exceed the limit", length));
        }
    }
}
