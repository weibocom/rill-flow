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

package com.weibo.rill.flow.impl.statistic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.common.constant.ReservedConstant;
import com.weibo.rill.flow.common.model.BusinessHeapStatus;
import com.weibo.rill.flow.common.model.ProfileType;
import com.weibo.rill.flow.impl.redis.JedisFlowClient;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.traversal.notify.NotifyType;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.statistic.SystemMonitorStatistic;
import com.weibo.rill.flow.service.storage.RuntimeRedisClients;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.service.util.ProfileUtil;
import com.weibo.rill.flow.service.util.PrometheusActions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


@Slf4j
@Component
public class SystemMonitorStatisticImpl implements SystemMonitorStatistic {
    private static final String EXECUTION_STATUS_FORMAT = "sta_rate_%s" + ReservedConstant.EXECUTION_ID_CONNECTOR + "%s";
    public static final String UNKNOWN = "unknown";

    private final Set<String> serviceIds = Sets.newConcurrentHashSet();
    private final SerializeConfig serializeConfig = new SerializeConfig();

    @Autowired
    private BizDConfs bizDConfs;
    @Autowired
    @Qualifier("runtimeRedisClients")
    private RuntimeRedisClients runtimeRedisClients;

    @PostConstruct
    public void init() {
        serializeConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
    }

    public void recordNotify(long executionCost, String executionId, NotifyType notifyType) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);

            if (notifyType == NotifyType.SUBMIT) {
                updateExecutionStatus(executionId);
            } else if (notifyType == NotifyType.REDO) {
                initExecutionStatus(executionId, serviceId);
                updateExecutionStatus(executionId);
            }

            ProfileActions.recordDagTotalExecutionTime(executionCost, serviceId);
            // 记录prometheus
            PrometheusActions.recordDagTotalExecutionTime(executionCost, serviceId);
        } catch (Exception e) {
            log.warn("recordNotify fails, executionCost:{}, executionId:{}, notifyType:{}, errorMsg:{}",
                    executionCost, executionId, notifyType, e.getMessage());
        }
    }

    public void recordTraversal(long executionCost, String executionId) {
        ProfileActions.recordDagTotalExecutionTime(executionCost, ExecutionIdUtil.getServiceId(executionId));
        // 记录prometheus
        PrometheusActions.recordDagTotalExecutionTime(executionCost, ExecutionIdUtil.getServiceId(executionId));
    }

    public void recordTaskRun(long executionCost, String executionId, TaskInfo taskInfo) {
        String serviceId = ExecutionIdUtil.getServiceId(executionId);
        ProfileActions.recordTaskTotalExecutionTime(executionCost, taskInfo.getTask().getCategory(), serviceId);
        ProfileActions.recordDagTotalExecutionTime(executionCost, serviceId);
        // 记录prometheus
        PrometheusActions.recordTaskTotalExecutionTime(executionCost, taskInfo.getTask().getCategory(), serviceId);
        PrometheusActions.recordDagTotalExecutionTime(executionCost, serviceId);
    }

    public void recordTaskCompliance(String executionId, TaskInfo taskInfo, boolean reached, long percentage) {
        String serviceId = ExecutionIdUtil.getServiceId(executionId);
        String categoryName = Optional.ofNullable(taskInfo).map(TaskInfo::getTask).map(BaseTask::getCategory).orElse(UNKNOWN);
        String taskName = Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(UNKNOWN);
        ProfileActions.recordTaskCompliance(serviceId, categoryName, taskName, reached, percentage);
        // 记录prometheus
        PrometheusActions.recordTaskCompliance(serviceId, categoryName, taskName, reached, percentage);
    }

    public void recordDAGFinish(String executionId, long executionCost, DAGStatus dagStatus, DAGInfo dagInfo) {
        try {
            String failCode = Optional.ofNullable(dagInfo)
                    .filter(it -> dagStatus == DAGStatus.FAILED)
                    .map(DAGInfo::getDagInvokeMsg)
                    .map(DAGInvokeMsg::getCode)
                    .filter(StringUtils::isNotBlank)
                    .orElse(null);
            updateExecutionStatus(executionId, dagStatus, failCode);
        } catch (Exception e) {
            log.warn("recordDAGFinish fails, executionCost:{}, executionId:{}, errorMsg:{}", executionCost, executionId, e.getMessage());
        }
    }

    public void logExecutionStatus() {
        Set<String> serviceIdsCopy = new HashSet<>(serviceIds);
        serviceIds.clear();
        serviceIdsCopy.forEach(serviceId -> {
            try {
                BusinessHeapStatus businessHeapStatus = collectBusinessHeap(serviceId, null, null);

                ProfileActions.recordExecutionStatus(DAGStatus.RUNNING, serviceId, businessHeapStatus.getRunningCount());
                ProfileActions.recordExecutionStatus(DAGStatus.SUCCEED, serviceId, businessHeapStatus.getSuccessCount());
                ProfileActions.recordExecutionStatus(DAGStatus.FAILED, serviceId, businessHeapStatus.getFailedCount());
                // 记录prometheus
                PrometheusActions.recordExecutionStatus(DAGStatus.RUNNING, serviceId, businessHeapStatus.getRunningCount());
                PrometheusActions.recordExecutionStatus(DAGStatus.SUCCEED, serviceId, businessHeapStatus.getSuccessCount());
                PrometheusActions.recordExecutionStatus(DAGStatus.FAILED, serviceId, businessHeapStatus.getFailedCount());
            } catch (Exception e) {
                log.warn("logExecutionStatus fails, serviceId:{}, errorMsg:{}", serviceId, e.getMessage());
            }
        });
    }

    public JSONObject businessHeapMonitor(List<String> serviceIds, Integer startTimeOffset, Integer endTimeOffset) {
        JSONObject ret = new JSONObject();
        Optional.ofNullable(serviceIds)
                .orElse(Collections.emptyList())
                .forEach(serviceId -> ret.put(serviceId, JSON.toJSON(collectBusinessHeap(serviceId, startTimeOffset, endTimeOffset), serializeConfig)));
        return ret;
    }

    @Override
    public Map<String, Object> getExecutionCountByStatus(BusinessHeapStatus businessHeapStatus, DAGStatus dagStatus) {
        String serviceId = businessHeapStatus.getServiceId();
        long startTime = businessHeapStatus.getStatisticTimePeriodStartTime();
        long endTime = businessHeapStatus.getStatisticTimePeriodEndTime();

        Map<String, Object> ret = Maps.newHashMap();
        if (dagStatus != null) {
            ret.put(dagStatus.getValue(), runtimeRedisClients.zcount(executionStatusKey(serviceId, dagStatus), startTime, endTime));
            return ret;
        } else {
            ret.put(DAGStatus.RUNNING.getValue(), runtimeRedisClients.zcount(executionStatusKey(serviceId, DAGStatus.RUNNING), startTime, endTime));
            ret.put(DAGStatus.SUCCEED.getValue(), runtimeRedisClients.zcount(executionStatusKey(serviceId, DAGStatus.SUCCEED), startTime, endTime));
            ret.put(DAGStatus.FAILED.getValue(), runtimeRedisClients.zcount(executionStatusKey(serviceId, DAGStatus.FAILED), startTime, endTime));
        }

        return ret;
    }

    public Map<String, Object> getExecutionCountByCode(BusinessHeapStatus businessHeapStatus, String code) {
        String serviceId = businessHeapStatus.getServiceId();
        long startTime = businessHeapStatus.getStatisticTimePeriodStartTime();
        long endTime = businessHeapStatus.getStatisticTimePeriodEndTime();

        Map<String, Object> ret = Maps.newHashMap();
        if (StringUtils.isNotBlank(code)) {
            ret.put(code, runtimeRedisClients.zcount(executionCodeKey(serviceId, code), startTime, endTime));
            return ret;
        }

        String totalCodeKey = totalCodeKey(serviceId);
        Set<String> allCodes = runtimeRedisClients.zrange(totalCodeKey, totalCodeKey, 0, -1);
        if (CollectionUtils.isEmpty(allCodes)) {
            return ret;
        }
        allCodes.forEach(it -> ret.put(it, runtimeRedisClients.zcount(executionCodeKey(serviceId, it), startTime, endTime)));
        return ret;
    }

    public List<Pair<String, String>> getExecutionIdsByStatus(String serviceId, DAGStatus dagStatus, Long cursor, Integer offset, Integer count) {
        return getExecutionIds(executionStatusKey(serviceId, dagStatus), cursor, offset, count);
    }

    public List<Pair<String, String>> getExecutionIdsByCode(String serviceId, String code, Long cursor, Integer offset, Integer count) {
        return getExecutionIds(executionCodeKey(serviceId, code), cursor, offset, count);
    }

    public List<Pair<String, String>> getExecutionIds(String key, Long cursor, Integer offset, Integer count) {
        return getExecutionIds(runtimeRedisClients.zrevrangeByScoreWithScores(key, cursor, 0, offset, count));
    }

    public List<Pair<String, String>> getExecutionIdsByStatus(String serviceId, DAGStatus dagStatus, Long cursor) {
        return getExecutionIds(executionStatusKey(serviceId, dagStatus), cursor);
    }

    public List<Pair<String, String>> getExecutionIds(String key, Long cursor) {
        return getExecutionIds(runtimeRedisClients.zrangeByScoreWithScores(key, 0, cursor));
    }

    private List<Pair<String, String>> getExecutionIds(Set<Pair<String, Double>> redisRet) {
        return redisRet.stream()
                .sorted((c1, c2) -> Double.compare(c2.getRight(), c1.getRight()))
                .map(memberToScore -> {
                    String executionId = memberToScore.getKey();
                    String time = String.valueOf(memberToScore.getValue().longValue());
                    return Pair.of(executionId, time);
                }).toList();
    }

    private void initExecutionStatus(String executionId, String serviceId) {
        runtimeRedisClients.zrem(executionStatusKey(serviceId, DAGStatus.SUCCEED), executionId);
        runtimeRedisClients.zrem(executionStatusKey(serviceId, DAGStatus.FAILED), executionId);
        String serviceKey = executionServiceKey(serviceId, ExecutionIdUtil.getSubmitTime(executionId));
        String failCode = getRuntimeClient(serviceKey).hget(serviceKey, executionId);
        if (StringUtils.isNotBlank(failCode)) {
            runtimeRedisClients.zrem(executionCodeKey(serviceId, failCode), executionId);
            runtimeRedisClients.hdel(serviceKey, executionId);
        }
    }

    private JedisFlowClient getRuntimeClient(String key) {
        return ((JedisFlowClient) runtimeRedisClients.choose(key));
    }

    private void updateExecutionStatus(String executionId) {
        updateExecutionStatus(executionId, DAGStatus.RUNNING, null);
    }

    private void updateExecutionStatus(String executionId, DAGStatus dagStatus, String failCode) {
        String serviceId = ExecutionIdUtil.getServiceId(executionId);
        long submitTime = ExecutionIdUtil.getSubmitTime(executionId);
        String businessId = ExecutionIdUtil.getBusinessIdFromServiceId(serviceId);
        int statisticSaveTimeInMinute = bizDConfs.getBusinessIdToStatisticLogSaveTimeInMinute().getOrDefault(businessId, 180);
        long minTime = System.currentTimeMillis() - (long) statisticSaveTimeInMinute * 60 * 1000;
        if (submitTime <= minTime) {
            return;
        }

        Optional.ofNullable(dagStatus).ifPresent(it -> {
            serviceIds.add(serviceId);
            if (dagStatus != DAGStatus.RUNNING) {
                runtimeRedisClients.zrem(executionStatusKey(serviceId, DAGStatus.RUNNING), executionId);
            }
            String statusKey = executionStatusKey(serviceId, dagStatus);
            JedisFlowClient jedisFlowClient = getRuntimeClient(statusKey);
            jedisFlowClient.pipelined().accept(pipeline -> {
                pipeline.zadd(statusKey, submitTime, executionId);
                pipeline.zremrangeByScore(statusKey, 0, minTime);
            });
        });

        Optional.ofNullable(failCode).filter(StringUtils::isNotBlank).ifPresent(code -> {
            String codeKey = executionCodeKey(serviceId, failCode);
            JedisFlowClient jedisFlowClient = getRuntimeClient(codeKey);
            jedisFlowClient.pipelined().accept(pipeline -> {
                pipeline.zadd(codeKey, submitTime, executionId);
                pipeline.zremrangeByScore(codeKey, 0, minTime);
            });

            String totalCodeKey = totalCodeKey(serviceId);
            JedisFlowClient totalCodeRedisClient = getRuntimeClient(totalCodeKey);
            totalCodeRedisClient.pipelined().accept(pipeline -> {
                pipeline.zadd(totalCodeKey, System.currentTimeMillis(), failCode);
                pipeline.zremrangeByScore(totalCodeKey, 0, minTime);
            });

            String serviceKey = executionServiceKey(serviceId, submitTime);
            getRuntimeClient(serviceKey).pipelined().accept(pipeline -> {
                pipeline.hset(serviceKey, executionId, failCode);
                pipeline.expire(serviceKey, statisticSaveTimeInMinute * 60L);
            });
        });
    }

    private BusinessHeapStatus collectBusinessHeap(String serviceId, Integer startTimeOffset, Integer endTimeOffset) {
        BusinessHeapStatus businessHeapStatus = calculateTimePeriod(serviceId, startTimeOffset, endTimeOffset);
        long startTime = businessHeapStatus.getStatisticTimePeriodStartTime();
        long endTime = businessHeapStatus.getStatisticTimePeriodEndTime();

        businessHeapStatus.setRunningCount(runtimeRedisClients.zcount(executionStatusKey(serviceId, DAGStatus.RUNNING), startTime, endTime));
        businessHeapStatus.setSuccessCount(runtimeRedisClients.zcount(executionStatusKey(serviceId, DAGStatus.SUCCEED), startTime, endTime));
        businessHeapStatus.setFailedCount(runtimeRedisClients.zcount(executionStatusKey(serviceId, DAGStatus.FAILED), startTime, endTime));

        return businessHeapStatus;
    }

    public BusinessHeapStatus calculateTimePeriod(String serviceId, Integer startTimeOffset, Integer endTimeOffset) {
        String businessId = ExecutionIdUtil.getBusinessIdFromServiceId(serviceId);
        Map<String, Integer> startTimeMap = bizDConfs.getLogStartTimeOffsetInMinute();
        Map<String, Integer> endTimeMap = bizDConfs.getLogEndTimeOffsetInMinute();

        int startTimeConfig = Optional.ofNullable(startTimeOffset).orElse(
                startTimeMap.getOrDefault(serviceId,
                        startTimeMap.getOrDefault(businessId, 10)));
        int endTimeConfig = Optional.ofNullable(endTimeOffset).orElse(
                endTimeMap.getOrDefault(serviceId,
                        endTimeMap.getOrDefault(businessId, 1)));
        long currentTime = System.currentTimeMillis();
        long startTime = currentTime - (long) startTimeConfig * 60 * 1000;
        long endTime = currentTime - (long) endTimeConfig * 60 * 1000;

        return BusinessHeapStatus.builder()
                .serviceId(serviceId)
                .collectTime(currentTime)
                .statisticDuration(startTimeConfig - endTimeConfig)
                .statisticTimePeriodStartTime(startTime)
                .statisticTimePeriodEndTime(endTime)
                .build();
    }

    private static String executionStatusKey(String serviceId, DAGStatus dagStatus) {
        return String.format(EXECUTION_STATUS_FORMAT, serviceId, dagStatus.getValue());
    }

    private static String executionCodeKey(String serviceId, String code) {
        return String.format(EXECUTION_STATUS_FORMAT, serviceId, code);
    }

    private static String totalCodeKey(String serviceId) {
        return String.format(EXECUTION_STATUS_FORMAT, serviceId, "code");
    }

    private static String executionServiceKey(String serviceId, long time) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String timeString = dateFormat.format(new Date(time));
        return String.format(EXECUTION_STATUS_FORMAT, serviceId, timeString);
    }

    public static class ProfileActions {
        private ProfileActions() {
            // Add a private constructor to hide the implicit public one.
        }

        private static final ProfileType DAG = new ProfileType("dag");
        private static final ProfileType DAG_TASK_COMPLIANCE = new ProfileType("dag", 0, 25, 50, 100, 50);
        private static final String DAG_TIME_FORMAT = "cost_%s";
        private static final String TASK_TIME_FORMAT = "%s_%s";
        private static final String TASK_COMPLIANCE_FORMAT = "compliance_%s_%s_%s";
        private static final String TASK_COMPLIANCE_PERCENTAGE_FORMAT = "compliance_percentage_%s_%s_%s";
        public static final String REACHED = "REACHED";
        public static final String NOT_REACHED = "NOTREACHED";

        public static void recordDagTotalExecutionTime(long executionTime, String serviceId) {
            String name = String.format(DAG_TIME_FORMAT, serviceId);
            ProfileUtil.count(DAG, name, System.currentTimeMillis(), (int) executionTime);
        }

        public static void recordTaskTotalExecutionTime(long executionCost, String action, String serviceId) {
            String name = String.format(TASK_TIME_FORMAT, action, serviceId);
            ProfileUtil.accessStatistic(DAG, name, System.currentTimeMillis(), executionCost);
        }

        public static void recordTaskCompliance(String serviceId, String category, String taskName, boolean reached, long percentage) {
            String countName = String.format(TASK_COMPLIANCE_FORMAT, category, serviceId, taskName);
            String percentageName = String.format(TASK_COMPLIANCE_PERCENTAGE_FORMAT, category, serviceId, taskName);
            ProfileUtil.accessStatistic(DAG_TASK_COMPLIANCE, percentageName, System.currentTimeMillis(), percentage);
            ProfileUtil.count(DAG_TASK_COMPLIANCE, countName + "_" + (reached ? REACHED : NOT_REACHED), System.currentTimeMillis(), 1);
        }

        public static void recordExecutionStatus(DAGStatus dagStatus, String serviceId, Long count) {
            int incrNum = Optional.ofNullable(count).orElse(0L).intValue();
            String name = executionStatusKey(serviceId, dagStatus);
            ProfileUtil.count(DAG, name, System.currentTimeMillis(), incrNum);
        }
    }
}
