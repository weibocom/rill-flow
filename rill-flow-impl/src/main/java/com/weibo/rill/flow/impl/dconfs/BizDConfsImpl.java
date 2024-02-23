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

package com.weibo.rill.flow.impl.dconfs;

import com.weibo.rill.flow.common.function.ResourceCheckConfig;
import com.weibo.rill.flow.common.util.SerializerUtil;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
@Getter
@Setter
public class BizDConfsImpl implements BizDConfs {
    private final Map<String, ResourceCheckConfig> resourceCheckIdToConfigBean = new ConcurrentHashMap<>();

    @Value("#{${weibo.flow.runtime.redis.storage.business.id.to.client.id:{:}}}")
    private Map<String, String> redisBusinessIdToClientId;

    @Value("#{${weibo.flow.runtime.redis.storage.service.id.to.client.id:{:}}}")
    private Map<String, String> redisServiceIdToClientId;

    @Value("#{${weibo.flow.runtime.redis.storage.business.id.to.finish.reserve.time:{:}}}")
    private Map<String, Integer> redisBusinessIdToFinishReserveSecond;

    @Value("#{${weibo.flow.runtime.redis.storage.business.id.to.unfinished.reserve.time:{:}}}")
    private Map<String, Integer> redisBusinessIdToUnfinishedReserveSecond;

    @Value("#{${weibo.flow.runtime.redis.storage.business.id.to.context.max.length:{:}}}")
    private Map<String, Integer> redisBusinessIdToContextMaxLength;

    @Value("#{${weibo.flow.business.id.to.runtime.submit.context.max.size:{:}}}")
    private Map<String, Integer> redisBusinessIdToRuntimeSubmitContextMaxSize;

    @Value("#{${weibo.flow.business.id.to.runtime.callback.context.max.size:{:}}}")
    private Map<String, Integer> redisBusinessIdToRuntimeCallbackContextMaxSize;

    @Value("#{${weibo.flow.runtime.redis.storage.business.id.to.dag.info.max.length:{:}}}")
    private Map<String, Integer> redisBusinessIdToDAGInfoMaxLength;

    @Value("#{${weibo.flow.runtime.swap.storage.business.id.to.client.id:{:}}}")
    private Map<String, String> swapBusinessIdToClientId;

    @Value("#{${weibo.flow.runtime.swap.storage.business.id.to.finish.reserve.time:{:}}}")
    private Map<String, Integer> swapBusinessIdToFinishReserveSecond;

    @Value("#{${weibo.flow.runtime.swap.storage.business.id.to.unfinished.reserve.time:{:}}}")
    private Map<String, Integer> swapBusinessIdToUnfinishedReserveSecond;

    @Value("#{${weibo.flow.task.score.exp.when.pop.service.id.to.exp:{:}}}")
    private Map<String, String> taskScoreExpWhenPop;

    @Value("#{'${weibo.flow.runtime.redis.storage.usage.check.ids:}'.split(',')}")
    private Set<String> runtimeRedisUsageCheckIDs;

    @Value("#{${weibo.flow.runtime.redis.storage.id.to.max.usage:{:}}}")
    private Map<String, Integer> runtimeRedisStorageIdToMaxUsage;

    @Value("${weibo.flow.runtime.redis.default.storage.max.usage:90}")
    private int runtimeRedisDefaultStorageMaxUsage;

    @Value("${weibo.flow.runtime.redis.customized.storage.max.usage:95}")
    private int runtimeRedisCustomizedStorageMaxUsage;

    @Value("${weibo.flow.runtime.resource.status.statistic.time:10800}")
    private int resourceStatusStatisticTimeInSecond;

    @Value("#{${weibo.flow.runtime.resource.check.id.to.config:{:}}}")
    private Map<String, String> resourceCheckIdToConfig;

    @Value("#{${weibo.flow.runtime.submit.traffic.limit.id.to.config:{:}}}")
    private Map<String, Integer> submitTrafficLimitIdToConfig;

    @Value("#{'${weibo.flow.rumtime.http.invoke.url.black.list:/2/flow/submit.json,/2/flow/redo.json}'.split(',')}")
    private Set<String> runtimeHttpInvokeUrlBlackList;

    @Value("${weibo.flow.runtime.submit.context.max.size:10240}")
    private int runtimeSubmitContextMaxSize;

    @Value("${weibo.flow.runtime.callback.context.max.size:6144}")
    private int runtimeCallbackContextMaxSize;

    @Value("#{${weibo.flow.statistic.info.save.time.in.minute:{:}}}")
    private Map<String, Integer> businessIdToStatisticLogSaveTimeInMinute;

    @Value("#{${weibo.flow.statistic.info.log.start.time.offset.in.minute:{:}}}")
    private Map<String, Integer> logStartTimeOffsetInMinute;

    @Value("#{${weibo.flow.statistic.info.log.end.time.offset.in.minute:{:}}}")
    private Map<String, Integer> logEndTimeOffsetInMinute;

    @Value("#{'${weibo.flow.sys.exp.filter.ignore.logs:}'.split(',')}")
    private Set<String> sysExpFilterIgnoreLogs;

    @Value("#{${weibo.flow.tenant.defined.task.invoke.profile.log:{:}}}")
    private Map<String, List<String>> tenantDefinedTaskInvokeProfileLog;

    @Value("${weibo.flow.dag.max.depth.config:5}")
    private int weiboFlowDAGMaxDepth;

    @Value("#{${weibo.flow.long.term.storage.business.id.to.client.id:{:}}}")
    private Map<String, String> longTermStorageBusinessIdToClientId;

    @Value("#{${weibo.flow.long.term.storage.business.id.to.reserve.time:{:}}}")
    private Map<String, Integer> longTermBusinessIdToReserveSecond;

    @Value("#{${weibo.flow.customized.storage.business.id.to.client.id:{:}}}")
    private Map<String, String> customizedBusinessIdToClientId;

    @Value("#{${weibo.flow.customized.storage.service.id.to.client.id:{:}}}")
    private Map<String, String> customizedServiceIdToClientId;

    @Value("#{${weibo.flow.customized.storage.business.id.to.reserve.time:{:}}}")
    private Map<String, Integer> customizedBusinessIdToReserveSecond;

    @Value("#{${weibo.flow.http.client.business.id.to.client.id:{:}}}")
    private Map<String, String> httpClientBusinessIdToClientId;

    @Value("#{${weibo.flow.auth.source.to.key:{:}}}")
    private Map<String, String> authSourceToKeyMap;

    @Override
    public int getFlowDAGMaxDepth() {
        return 5;
    }

    @PostConstruct
    public void generateResourceCheckIdToConfigBean() {
        resourceCheckIdToConfigBean.clear();

        if (MapUtils.isEmpty(resourceCheckIdToConfig)) {
            return;
        }
        resourceCheckIdToConfig.forEach((key, value) -> {
            try {
                resourceCheckIdToConfigBean.put(key, SerializerUtil.deserialize(value.getBytes(StandardCharsets.UTF_8), ResourceCheckConfig.class));
            } catch (Exception e) {
                log.warn("BizDConfs resourceCheckIdToConfigBean fails, key:{}, value:{}", key, value);
            }
        });

        log.info("BizDConfs init finished, init resourceCheckIdToConfigBean={}", resourceCheckIdToConfigBean);
    }
}
