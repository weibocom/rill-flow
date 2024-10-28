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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.DAGStorageKeysUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.weibo.rill.flow.service.util.DAGStorageKeysUtil.MD5_PREFIX;

/**
 * <pre>
 * DAG描述符规则
 * 1. DAG描述符id
 *    举例:
 *      businessId:feature:alias 如: testBusinessId:testFeatureName:release
 *      businessId:feature 如: testBusinessId:testFeatureName 按testFeatureName feature灰度策略选择alias 无灰度策略则alias用release
 *      businessId:feature:md5_描述符md5 如: testBusinessId:testFeatureName:md5_4297f44b13955235245b2497399d7a93
 *    1.1 businessId
 *        含义: 表示业务类型
 *        redisKey: business_id
 *        type: set
 *        member: 业务名称 如: testBusinessId
 *    1.2 feature
 *        含义: 表示业务的一种服务特征
 *        redisKey: feature_ + businessId 如: feature_testBusinessId
 *        type: set
 *        member: 服务名称 如: testFeatureName
 *    1.3 alias
 *        含义: 服务的描述符别名
 *        redisKey: alias_ + businessId + _ + feature 如: alias_testBusinessId_testFeatureName
 *        type: set
 *        member: 别名id 如: release/preview
 *    1.4 version
 *        含义: 别名下的各个版本
 *        redisKey: version_ + businessId + _ + feature + _ + alias 如: version_testBusinessId_testFeatureName_release
 *        type: zset
 *        member: 描述符md5
 *        score: 时间毫秒数
 *    1.5 gray
 *        含义: 服务灰度策略
 *        redisKey: gray_ + businessId + _ + feature 如: gray_testBusinessId_testFeatureName
 *        type: hash
 *        field: 别名
 *        value: 别名灰度策略
 *
 * 2. DAG描述符
 *    redisKey: descriptor_ + businessId + _ + feature + _ + 描述符md5 如: descriptor_testBusinessId_testFeatureName_md5
 *    type: string
 *    value: DAG描述符
 *
 * 3. 同一个业务的feature/alias/version/gray/DAG描述符 存储在一个端口上
 * </pre>
 * <p>
 * Created by xilong on 2021/8/18.
 */
@Component
@Slf4j
public class DAGDescriptorDAO {

    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;
    @Autowired
    private SwitcherManager switcherManagerImpl;

    private final Cache<String, DescriptorPO> descriptorRedisKeyToYamlCache = CacheBuilder.newBuilder()
            .maximumSize(300)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .build();

    @Setter
    private int versionMaxCount = 300;

    private static final String VERSION_ADD = """
            local maxVersionCount = ARGV[1];
            local versionKey = KEYS[1];

            local versionToDel = redis.call("zrange", versionKey, 0, -maxVersionCount);
            for i = 1, #versionToDel, 1 do
                local md5 = versionToDel[i];
                redis.call("zrem", versionKey, md5);
            end

            redis.call("zadd", versionKey, ARGV[2], ARGV[3]);
            redis.call("set", KEYS[2], ARGV[4]);

            return "OK";""";

    public DescriptorPO getDescriptorPO(String dagDescriptorId, String descriptorRedisKey, String businessId) throws ExecutionException {
        // 根据redisKey获取文件内容
        DescriptorPO descriptorPO = switcherManagerImpl.getSwitcherState("ENABLE_GET_DESCRIPTOR_FROM_CACHE") ?
                descriptorRedisKeyToYamlCache.get(descriptorRedisKey, () -> getDescriptorPO(businessId, descriptorRedisKey)) :
                getDescriptorPO(businessId, descriptorRedisKey);
        if (descriptorPO == null || StringUtils.isEmpty(descriptorPO.getDescriptor())) {
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("descriptor:%s value empty", dagDescriptorId));
        }
        return descriptorPO;
    }

    private DescriptorPO getDescriptorPO(String businessId, String descriptorRedisKey) {
        String descriptor = redisClient.get(businessId, descriptorRedisKey);
        return descriptor == null? null: new DescriptorPO(descriptor);
    }

    public String persistDescriptorPO(String businessId, String featureName, String alias, DescriptorPO descriptorPO) {
        String descriptor = descriptorPO.getDescriptor();
        String md5 = DigestUtils.md5Hex(descriptor);

        List<String> keys = Lists.newArrayList();
        List<String> argv = Lists.newArrayList();
        keys.add(DAGStorageKeysUtil.buildVersionRedisKey(businessId, featureName, alias));
        keys.add(DAGStorageKeysUtil.buildDescriptorRedisKey(businessId, featureName, md5));
        argv.add(String.valueOf(versionMaxCount));
        argv.add(String.valueOf(System.currentTimeMillis()));
        argv.add(md5);
        argv.add(descriptor);
        redisClient.eval(VERSION_ADD, businessId, keys, argv);

        return DAGStorageKeysUtil.buildDescriptorId(businessId, featureName, MD5_PREFIX + md5);
    }
}
