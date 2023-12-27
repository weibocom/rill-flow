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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.function.ResourceCheckConfig;
import com.weibo.rill.flow.common.function.ResourceStatus;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.util.SerializerUtil;
import com.weibo.rill.flow.service.configuration.BeanConfig;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.dconfs.DynamicClientConfs;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
@Service
public class DAGSubmitChecker {
    @Autowired
    private BizDConfs bizDConfs;
    @Autowired
    private DAGResourceStatistic dagResourceStatistic;
    @Autowired
    private TrafficRateLimiter trafficRateLimiter;
    @Autowired
    private SystemMonitorStatistic systemMonitorStatistic;
    @Autowired
    private DynamicClientConfs dynamicClientConfs;
    @Autowired
    private SwitcherManager switcherManagerImpl;

    public ResourceCheckConfig getCheckConfig(String resourceCheck) {
        if (StringUtils.isBlank(resourceCheck)) {
            return null;
        }
        try {
            return SerializerUtil.deserialize(resourceCheck.getBytes(StandardCharsets.UTF_8), ResourceCheckConfig.class);
        } catch (Exception e) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "resource_check content nonsupport");
        }
    }

    public void check(String executionId, ResourceCheckConfig resourceCheckConfig) {
        try {
            doCheck(executionId, resourceCheckConfig);
        } catch (TaskException taskException) {
            log.warn("submit executionId:{}, msg:{}", executionId, taskException.getMessage());
            throw taskException;
        } catch (Exception e) {
            log.warn("check fails, executionId:{}", executionId, e);
        }
    }

    private void doCheck(String executionId, ResourceCheckConfig resourceCheckConfig) {
        String serviceId = ExecutionIdUtil.getServiceId(executionId);
        String businessId = ExecutionIdUtil.getBusinessIdFromServiceId(serviceId);

        StorageCheck storageCheck = storageUsageCheck(executionId, serviceId, businessId);
        if (storageCheck.isUsageLimit()) {
            throw new TaskException(BizError.ERROR_RUNTIME_STORAGE_USAGE_LIMIT.getCode(), "runtime storage limit");
        }

        ResourceCheck resourceCheck = resourceStatusCheck(serviceId, businessId, resourceCheckConfig);
        if (resourceCheck.isResourceLimit()) {
            String resourceNames = String.join(",", resourceCheck.getUnsatisfiedResources());
            throw new TaskException(BizError.ERROR_RUNTIME_RESOURCE_STATUS_LIMIT.getCode(), "resource limit resources: " + resourceNames);
        }

        if (switcherManagerImpl.getSwitcherState("ENABLE_RUNTIME_SUBMIT_TRAFFIC_CONTROL")) {
            trafficControl(executionId, serviceId, businessId);
        }

        FlowCheck flowCheck = flowRuntimeCheck(businessId, serviceId);
        DecimalFormat numberFormat = new DecimalFormat();
        numberFormat.setMaximumFractionDigits(2);
        numberFormat.setGroupingSize(0);
        numberFormat.setRoundingMode(RoundingMode.FLOOR);
        if (flowCheck.isCompleteRateLimit()) {
            String msg = "complete rate:" + numberFormat.format(flowCheck.getCompleteRate()) + "% lower than config:" + flowCheck.getCompleteRateThreshold() + "%";
            throw new TaskException(BizError.ERROR_RUNTIME_RESOURCE_STATUS_LIMIT.getCode(), msg);
        }
        if (flowCheck.isSuccessRateLimit()) {
            String msg = "success rate:" + numberFormat.format(flowCheck.getSuccessRate()) + "% lower than config:" + flowCheck.getSuccessRateThreshold() + "%";
            throw new TaskException(BizError.ERROR_RUNTIME_RESOURCE_STATUS_LIMIT.getCode(), msg);
        }
        if (flowCheck.isHeapLimit()) {
            String msg = "heap count:" + flowCheck.getHeapCount() + " higher than config:" + flowCheck.getHeapCountThreshold();
            throw new TaskException(BizError.ERROR_RUNTIME_RESOURCE_STATUS_LIMIT.getCode(), msg);
        }
    }

    public Map<String, Object> getCheckRet(String businessId, String serviceId, ResourceCheckConfig resourceCheckConfig) {
        String executionId = ExecutionIdUtil.generateExecutionId(serviceId);
        Map<String, Object> ret = Maps.newHashMap();
        ret.put("storage_check", storageUsageCheck(executionId, serviceId, businessId));
        ret.put("resource_check", resourceStatusCheck(serviceId, businessId, resourceCheckConfig));
        ret.put("flow_check", flowRuntimeCheck(businessId, serviceId));
        return ret;
    }

    private StorageCheck storageUsageCheck(String executionId, String serviceId, String businessId) {
        StorageCheck storageCheck = StorageCheck.builder().usageLimit(false).build();
        if (!switcherManagerImpl.getSwitcherState("ENABLE_RUNTIME_STORAGE_USAGE_CHECK")) {
            log.info("storageUsageCheck skip, executionId:{}", executionId);
            return storageCheck;
        }

        int maxUsagePercent = getRedisMaxUsagePercent(serviceId, businessId);
        storageCheck.setMaxUsagePercent(maxUsagePercent);
        if (maxUsagePercent <= 0 || maxUsagePercent >= 100) {
            return storageCheck;
        }

        int currentUsagePercent = dagResourceStatistic.getRuntimeRedisUsagePercent(executionId, serviceId);
        storageCheck.setCurrentUsagePercent(currentUsagePercent);

        storageCheck.setUsageLimit(currentUsagePercent > maxUsagePercent);

        return storageCheck;
    }

    private int getRedisMaxUsagePercent(String serviceId, String businessId) {
        if (bizDConfs.getRedisServiceIdToClientId().containsKey(serviceId)) {
            return bizDConfs.getRuntimeRedisUsageCheckIDs().contains(serviceId) ?
                    bizDConfs.getRuntimeRedisStorageIdToMaxUsage().getOrDefault(serviceId, bizDConfs.getRuntimeRedisCustomizedStorageMaxUsage()) : -1;
        }

        if (bizDConfs.getRedisBusinessIdToClientId().containsKey(businessId)) {
            return bizDConfs.getRuntimeRedisUsageCheckIDs().contains(businessId) ?
                    bizDConfs.getRuntimeRedisStorageIdToMaxUsage().getOrDefault(businessId, bizDConfs.getRuntimeRedisCustomizedStorageMaxUsage()) : -1;
        }

        return bizDConfs.getRuntimeRedisDefaultStorageMaxUsage();
    }

    private ResourceCheck resourceStatusCheck(String serviceId, String businessId, ResourceCheckConfig resourceCheckConfig) {
        ResourceCheck resourceCheck = ResourceCheck.builder().resourceLimit(false).build();
        if (!switcherManagerImpl.getSwitcherState("ENABLE_RUNTIME_RESOURCE_STATUS_CHECK")) {
            log.info("resourceStatusCheck current skip");
            return resourceCheck;
        }

        Set<String> unsatisfiedResources = getUnsatisfiedResources(serviceId, businessId, resourceCheckConfig);
        resourceCheck.setUnsatisfiedResources(unsatisfiedResources);
        resourceCheck.setResourceLimit(CollectionUtils.isNotEmpty(unsatisfiedResources));

        return resourceCheck;
    }

    private Set<String> getUnsatisfiedResources(String serviceId, String businessId, ResourceCheckConfig resourceCheckConfig) {
        ResourceCheckConfig checkConfig = getCheckConfig(serviceId, businessId, resourceCheckConfig);
        if (checkConfig == null || checkConfig.getCheckType() == ResourceCheckConfig.CheckType.SKIP) {
            return Collections.emptySet();
        }

        Collection<ResourceStatus> allResources = dagResourceStatistic.getDependentResources(serviceId).values();
        if (CollectionUtils.isEmpty(allResources)) {
            return Collections.emptySet();
        }

        return getLimitedResources(checkConfig, allResources);
    }

    private Set<String> getLimitedResources(ResourceCheckConfig checkConfig, Collection<ResourceStatus> allResources) {
        long currentTime = System.currentTimeMillis();
        List<ResourceStatus> limitedResources = allResources.stream()
                .filter(resourceStatus -> currentTime < resourceStatus.getResourceLimitedTime())
                .toList();

        ResourceCheckConfig.CheckType checkType = checkConfig.getCheckType();

        if (checkType == ResourceCheckConfig.CheckType.SHORT_BOARD ||
                (checkType == ResourceCheckConfig.CheckType.LONG_BOARD && allResources.size() == limitedResources.size())) {
            return limitedResources.stream()
                    .map(ResourceStatus::getResourceName)
                    .collect(Collectors.toSet());
        }
        if (checkType == ResourceCheckConfig.CheckType.KEY_RESOURCE && CollectionUtils.isNotEmpty(checkConfig.getKeyResources())) {
            return limitedResources.stream()
                    .map(ResourceStatus::getResourceName)
                    .filter(resourceName -> checkConfig.getKeyResources().contains(resourceName))
                    .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    private ResourceCheckConfig getCheckConfig(String serviceId, String businessId, ResourceCheckConfig resourceCheckConfig) {
        if (resourceCheckConfig != null) {
            return resourceCheckConfig;
        }

        ResourceCheckConfig customizedConfig = bizDConfs.getResourceCheckIdToConfigBean()
                .getOrDefault(serviceId, bizDConfs.getResourceCheckIdToConfigBean().get(businessId));
        if (customizedConfig != null) {
            return customizedConfig;
        }

        return ResourceCheckConfig.builder().checkType(ResourceCheckConfig.CheckType.SHORT_BOARD).build();
    }

    private void trafficControl(String executionId, String serviceId, String businessId) {
        if (needLimitTraffic(executionId, serviceId, businessId)) {
            throw new TaskException(BizError.ERROR_RUNTIME_RESOURCE_STATUS_LIMIT.getCode(), "submit traffic limit");
        }
    }

    private boolean needLimitTraffic(String executionId, String serviceId, String businessId) {
        Integer maxRate = bizDConfs.getSubmitTrafficLimitIdToConfig().get(serviceId);
        if (maxRate != null) {
            return !trafficRateLimiter.tryAcquire(executionId, serviceId, maxRate);
        }

        maxRate = bizDConfs.getSubmitTrafficLimitIdToConfig().get(businessId);
        if (maxRate != null) {
            return !trafficRateLimiter.tryAcquire(executionId, businessId, maxRate);
        }

        return false;
    }

    private FlowCheck flowRuntimeCheck(String businessId, String serviceId) {
        FlowCheck flowCheck = FlowCheck.builder().completeRateLimit(false).successRateLimit(false).heapLimit(false).build();
        if (!switcherManagerImpl.getSwitcherState("ENABLE_RUNTIME_SUBMIT_DAG_CHECK")) {
            log.info("flowRuntimeCheck current skip");
            return flowCheck;
        }

        // serviceId中包含冒号，该字符在spring spel表达式中有特殊含义，为配置方便dconfs中将:改为_
        String serviceIdInConfig = serviceId.replace(":", "_");
        String key = dynamicClientConfs.getSubmitConfig().containsKey(serviceIdInConfig) ? serviceIdInConfig : businessId;
        BeanConfig.Submit submit = Optional.ofNullable(dynamicClientConfs.getSubmitConfig().get(key)).map(BeanConfig::getSubmit).orElse(null);
        if (submit == null) {
            return flowCheck;
        }

        JSONObject ret = systemMonitorStatistic.businessHeapMonitor(Lists.newArrayList(serviceId), null, null);
        JSONObject serviceHeap = ret.getJSONObject(serviceId);
        int successCount = serviceHeap.getIntValue("success_count");
        int failedCount = serviceHeap.getIntValue("failed_count");
        int runningCount = serviceHeap.getIntValue("running_count");
        int allCount = successCount + failedCount + runningCount;
        int completeCount = successCount + failedCount;

        Optional.ofNullable(submit.getCompleteRateThreshold()).filter(it -> allCount > 0).ifPresent(completeRateThreshold -> {
            flowCheck.setCompleteRate(((double) completeCount) / allCount * 100);
            flowCheck.setCompleteRateThreshold(completeRateThreshold);
            flowCheck.setCompleteRateLimit(flowCheck.getCompleteRate() < completeRateThreshold);
        });

        Optional.ofNullable(submit.getSuccessRateThreshold()).filter(it -> completeCount > 0).ifPresent(successRateThreshold -> {
            flowCheck.setSuccessRate(((double) successCount) / completeCount * 100);
            flowCheck.setSuccessRateThreshold(successRateThreshold);
            flowCheck.setSuccessRateLimit(flowCheck.getSuccessRate() < successRateThreshold);
        });

        Optional.ofNullable(submit.getHeapThreshold()).ifPresent(heapThreshold -> {
            flowCheck.setHeapCount(runningCount);
            flowCheck.setHeapCountThreshold(heapThreshold);
            flowCheck.setHeapLimit(runningCount > heapThreshold);
        });

        return flowCheck;
    }

    @Getter
    @Setter
    @Builder
    public static class StorageCheck {
        private int maxUsagePercent;
        private int currentUsagePercent;
        private boolean usageLimit;
    }

    @Getter
    @Setter
    @Builder
    public static class ResourceCheck {
        private Set<String> unsatisfiedResources;
        private boolean resourceLimit;
    }

    @Getter
    @Setter
    @Builder
    public static class FlowCheck {
        private double completeRate;
        private double completeRateThreshold;
        private double successRate;
        private double successRateThreshold;
        private int heapCount;
        private int heapCountThreshold;
        private boolean completeRateLimit;
        private boolean successRateLimit;
        private boolean heapLimit;
    }
}
