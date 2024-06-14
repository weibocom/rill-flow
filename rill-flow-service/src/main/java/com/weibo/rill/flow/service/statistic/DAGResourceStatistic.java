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

package com.weibo.rill.flow.service.statistic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.function.ResourceStatus;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Slf4j
@Service
public class DAGResourceStatistic {
    private static final String INFO = "return redis.call(\"info\", unpack(ARGV));";
    public static final String CACHED_TASK_NAME_FORMAT = "%s#%s";

    @Autowired
    @Qualifier("runtimeRedisClients")
    private RedisClient runtimeRedisClients;
    @Autowired
    private BizDConfs bizDConfs;

    private final Cache<String, Integer> usagePercentCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    private final LoadingCache<String, ConcurrentMap<String, ResourceStatus>> serviceResourceCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(new CacheLoader<>() {
                @Override
                public ConcurrentMap<String, ResourceStatus> load(@NotNull String serviceId) throws Exception {
                    return new ConcurrentHashMap<>();
                }
            });

    public int getRuntimeRedisUsagePercent(String executionId, String serviceId) {
        try {
            return usagePercentCache.get(serviceId, () -> redisUsagePercent(executionId));
        } catch (Exception e) {
            log.warn("getRuntimeRedisUsagePercent fails, executionId:{}", executionId, e);
            return 0;
        }
    }

    public Map<String, ConcurrentMap<String, ResourceStatus>> getDependentResourcesWithServiceId() {
        return serviceResourceCache.asMap();
    }

    public Map<String, ResourceStatus> getDependentResources(String serviceId) {
        Map<String, ResourceStatus> resources = serviceResourceCache.getIfPresent(serviceId);
        if (MapUtils.isEmpty(resources)) {
            return Collections.emptyMap();
        }

        long statisticStartTime = System.currentTimeMillis() - bizDConfs.getResourceStatusStatisticTimeInSecond() * 1000L;
        Map<String, ResourceStatus> ret = resources.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().getUpdateTime() > statisticStartTime)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        resources.keySet().stream()
                .filter(key -> !ret.containsKey(key))
                .toList()
                .forEach(resources::remove);

        return ret;
    }

    public Map<String, Map<String, ResourceStatus>> orderDependentResources(String serviceId) {
        Map<String, Map<String, ResourceStatus>> resourceOrder = Maps.newHashMap();

        Map<String, ResourceStatus> taskNameToResourceStatus = getDependentResources(serviceId);
        resourceOrder.put("task_name_order", taskNameToResourceStatus);

        Map<String, ResourceStatus> resourceNameToResourceStatus = taskNameToResourceStatus.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getValue().getResourceName(),
                        entry -> ResourceStatus.builder()
                                .taskNames(Lists.newArrayList(entry.getKey()))
                                .resourceLimitedTime(entry.getValue().getResourceLimitedTime())
                                .resourceScore(entry.getValue().getResourceScore())
                                .updateTime(entry.getValue().getUpdateTime()).build(),
                        (v1, v2) -> {
                            ResourceStatus merge = v1.getUpdateTime() > v2.getUpdateTime() ? v1 : v2;
                            List<String> taskNames = Lists.newArrayList();
                            Optional.ofNullable(v1.getTaskNames()).ifPresent(taskNames::addAll);
                            Optional.ofNullable(v2.getTaskNames()).ifPresent(taskNames::addAll);
                            merge.setTaskNames(taskNames);
                            return merge;
                        }
                ));
        resourceOrder.put("resource_name_order", resourceNameToResourceStatus);
        return resourceOrder;
    }

    public boolean clearRuntimeResources(String serviceId, boolean clearAll, List<String> resourceNames) {
        try {
            if (clearAll) {
                serviceResourceCache.invalidate(serviceId);
            } else {
                Map<String, ResourceStatus> resources = serviceResourceCache.getIfPresent(serviceId);
                if (MapUtils.isNotEmpty(resources) && CollectionUtils.isNotEmpty(resourceNames)) {
                    resources.values().stream()
                            .filter(resourceStatus -> resourceNames.contains(resourceStatus.getResourceName()))
                            .forEach(resourceStatus -> {
                                resourceStatus.setResourceLimitedTime(0L);
                                resourceStatus.setUpdateTime(System.currentTimeMillis());
                            });
                }
            }
            return true;
        } catch (Exception e) {
            log.warn("clearRuntimeResources fails, serviceId:{}, clearAll:{}, resourceNames:{}", serviceId, clearAll, resourceNames, e);
            return false;
        }
    }

    public void updateUrlTypeResourceStatus(String executionId, String taskName, String resourceName, String urlRet) {
        try {
            if (StringUtils.isBlank(urlRet) || urlRet.startsWith("[")) {
                return;
            }

            JSONObject urlRetJson = JSON.parseObject(urlRet);
            updateUrlTypeResourceStatus(executionId, taskName, resourceName, urlRetJson);
        } catch (Exception e) {
            log.warn("updateUrlTypeResourceStatus fails, executionId:{}, resourceName:{}, urlRet:{}, errorMsg:{}",
                    executionId, resourceName, urlRet, e.getMessage());
        }
    }

    public void updateUrlTypeResourceStatus(String executionId, String taskName, String resourceName, JSONObject urlRet) {
        try {
            if (StringUtils.isBlank(taskName) || StringUtils.isBlank(resourceName)) {
                return;
            }

            long updateTime = System.currentTimeMillis();
            ResourceStatus resourceStatus = getResourceStatus(executionId, taskName, resourceName);
            resourceStatus.setUpdateTime(updateTime);

            int retryIntervalSeconds = getRetryIntervalSeconds(urlRet);
            if (retryIntervalSeconds > 0) {
                resourceStatus.setResourceLimitedTime(updateTime + retryIntervalSeconds * 1000L);
                log.info("update function url resource limit, executionId:{}, resourceName:{}, retryIntervalSeconds:{}",
                        executionId, resourceName, retryIntervalSeconds);
            }
        } catch (Exception e) {
            log.warn("updateUrlTypeResourceStatus fails, executionId:{}, resourceName:{}, errorMsg:{}",
                    executionId, resourceName, e.getMessage());
        }
    }

    private static int getRetryIntervalSeconds(JSONObject urlRet) {
        return Optional.ofNullable(urlRet)
                .map(it -> it.containsKey("data") && it.get("data") instanceof Map<?,?> ? it.getJSONObject("data") : it)
                .map(it -> it.getJSONObject("sys_info"))
                .map(it -> it.getInteger("retry_interval_seconds"))
                .orElseGet(() -> Optional.ofNullable(urlRet)
                        .map(it -> it.getJSONObject("error_detail"))
                        .map(it -> it.getInteger("retry_interval_seconds"))
                        .orElse(0));
    }

    public void updateFlowTypeResourceStatus(String executionId, String taskName, String resourceName, DAG dag) {
        try {
            if (StringUtils.isBlank(taskName) || StringUtils.isBlank(resourceName)) {
                return;
            }

            long updateTime = System.currentTimeMillis();
            ResourceStatus resourceStatus = getResourceStatus(executionId, taskName, resourceName);
            resourceStatus.setUpdateTime(updateTime);

            String flowServiceId = ExecutionIdUtil.generateServiceId(dag);
            Map<String, ResourceStatus> flowTaskNameToResourceStatus = getDependentResources(flowServiceId);
            flowTaskNameToResourceStatus.values().stream()
                    .filter(it -> it != null && it.getResourceLimitedTime() > updateTime)
                    .max(Comparator.comparingLong(ResourceStatus::getResourceLimitedTime))
                    .ifPresent(it -> {
                        resourceStatus.setResourceLimitedTime(it.getResourceLimitedTime());
                        log.info("update function flow resource limit, executionId:{}, resourceName:{}, retryIntervalSeconds:{}",
                                executionId, resourceName, (it.getResourceLimitedTime() - updateTime) / 1000);
                    });
        } catch (Exception e) {
            log.warn("updateFlowTypeResourceStatus fails, executionId:{}, resourceName:{}, errorMsg:{}",
                    executionId, resourceName, e.getMessage());
        }
    }

    private int redisUsagePercent(String executionId) {
        try {
            byte[] memoryInfo = (byte[]) runtimeRedisClients.eval(INFO, executionId, Collections.emptyList(), Lists.newArrayList("memory"));
            String memory = new String(memoryInfo, StandardCharsets.UTF_8);
            long usedMemory = Arrays.stream(memory.split("\n"))
                    .map(String::trim)
                    .filter(it -> it.startsWith("used_memory"))
                    .map(it -> it.split(":"))
                    .filter(array -> array.length > 1)
                    .map(array -> Long.valueOf(array[1]))
                    .findFirst()
                    .orElseGet(() -> {
                        log.warn("redis memory check can not get used memory, executionId:{}", executionId);
                        return 0L;
                    });

            List<String> maxMemoryConfig = runtimeRedisClients.configGet(executionId, "maxmemory");
            long maxMemory = Optional.ofNullable(maxMemoryConfig)
                    .filter(it -> CollectionUtils.isNotEmpty(it) && it.size() > 1)
                    .map(it -> Long.parseLong(it.get(1)))
                    .orElseGet(() -> {
                        log.warn("redis memory check can not get maxmemory, executionId:{}", executionId);
                        return Long.MAX_VALUE;
                    });

            int usagePercent = (int) (usedMemory * 100 / maxMemory);
            log.info("redis memory check executionId:{}, usagePercent:{}", executionId, usagePercent);
            return usagePercent;
        } catch (Exception e) {
            log.warn("can not get redis memory usage, executionId:{}", executionId, e);
            return 0;
        }
    }

    private ResourceStatus getResourceStatus(String executionId, String taskName, String resourceName) throws ExecutionException {
        Map<String, ResourceStatus> taskNameToResourceStatus = serviceResourceCache.get(ExecutionIdUtil.getServiceId(executionId));
        String cachedTaskName = String.format(CACHED_TASK_NAME_FORMAT, DAGWalkHelper.getInstance().getBaseTaskName(taskName), resourceName);
        return taskNameToResourceStatus.computeIfAbsent(cachedTaskName, key -> ResourceStatus.builder().resourceName(resourceName).build());
    }
}
