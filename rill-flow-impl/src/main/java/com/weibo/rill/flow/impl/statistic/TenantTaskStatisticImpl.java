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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.model.ProfileType;
import com.weibo.rill.flow.impl.redis.JedisFlowClient;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic;
import com.weibo.rill.flow.service.storage.RuntimeRedisClients;
import com.weibo.rill.flow.service.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@Service
public class TenantTaskStatisticImpl implements TenantTaskStatistic {
    private static final String BUSINESS_AGGREGATE_KEY = "%s_%s";
    private static final String BUSINESS_AGGREGATE_TASK_COUNT_FIELD = "%s_task_%s_count";
    private static final String BUSINESS_AGGREGATE_RESOURCE_COUNT_FIELD = "%s_resource_%s_count";
    private static final String BUSINESS_AGGREGATE_RESOURCE_EXECUTE_TIME_FIELD = "%s_resource_%s_execute_time";
    private static final String BUSINESS_AGGREGATE_RESOURCE_WAIT_TIME_FIELD = "%s_resource_%s_wait_time";
    private static final int BUSINESS_AGGREGATE_EXPIRE_TIME_IN_SECOND = 180 * 24 * 3600;
    private static final String FLOW_AGGREGATE_KEY = "fa_%s";
    private static final String FLOW_AGGREGATE_RESOURCE_EXECUTE_TIME_FIELD = "%s_execute_time";
    private static final String FLOW_AGGREGATE_RESOURCE_WAIT_TIME_FIELD = "%s_wait_time";
    private static final ProfileType TENANT = new ProfileType("tenant");
    private static final String TENANT_TASK_EXECUTION = "task_%s_%s_%s";
    private static final String TENANT_TASK_STASH_EXECUTION = "stash_%s_%s_%s_%s";
    private static final String TENANT_FLOW_STASH_EXECUTION = "stash_%s_%s_%s";

    private static final String TENANT_STR = "tenant_";


    private final Map<Pair<String, String>, AtomicLong> fieldIncrValue = new ConcurrentHashMap<>();
    @Autowired
    private BizDConfs bizDConfs;
    @Autowired
    private SwitcherManager switcherManagerImpl;
    @Autowired
    @Qualifier("dagDefaultStorageRedisClient")
    private JedisFlowClient businessAggregateClient;
    @Autowired
    @Qualifier("runtimeRedisClients")
    private RuntimeRedisClients runtimeRedisClients;

    public void recordTaskRun(long executionTime, String executionId, TaskInfo taskInfo) {
        taskProfileLog(executionTime, executionId, taskInfo.getName(), "submit");
        taskRunCount(executionId, taskInfo);
    }

    public void taskProfileLog(long executionTime, String executionId, String taskInfoName, String taskExecutionType) {
        if (StringUtils.isBlank(taskInfoName) || StringUtils.isBlank(taskExecutionType)) {
            return;
        }

        String baseTaskName = DAGWalkHelper.getInstance().getBaseTaskName(taskInfoName);
        String serviceId = ExecutionIdUtil.getServiceId(executionId);
        String businessId = ExecutionIdUtil.getBusinessIdFromServiceId(serviceId);

        Optional.ofNullable(bizDConfs.getTenantDefinedTaskInvokeProfileLog())
                .map(it -> it.get(businessId))
                .filter(it -> it.contains(baseTaskName))
                .ifPresent(it -> {
                    String name = String.format(TENANT_TASK_EXECUTION, serviceId, baseTaskName, taskExecutionType);
                    ProfileUtil.accessStatistic(TENANT, name, System.currentTimeMillis(), executionTime);
                    // 记录prometheus
                    PrometheusUtil.statisticsTotalTime(PrometheusActions.METER_PREFIX + TENANT_STR + name, executionTime);
                });
    }

    public void recordTaskStashProfileLog(long executionTime, String executionId, String taskInfoName, String stashType, boolean isSuccess) {
        if (StringUtils.isBlank(taskInfoName) || StringUtils.isBlank(stashType)) {
            return;
        }

        String baseTaskName = DAGWalkHelper.getInstance().getBaseTaskName(taskInfoName);
        String serviceId = ExecutionIdUtil.getServiceId(executionId);

        String name = String.format(TENANT_TASK_STASH_EXECUTION, serviceId, baseTaskName, stashType, isSuccess ? "success" : "fail");
        ProfileUtil.accessStatistic(TENANT, name, System.currentTimeMillis(), executionTime);
        // 记录prometheus
        PrometheusUtil.statisticsTotalTime(PrometheusActions.METER_PREFIX + TENANT_STR + name, executionTime);
    }

    public void recordFlowStashProfileLog(long executionTime, String executionId, String stashType, boolean isSuccess) {
        if (StringUtils.isBlank(executionId)) {
            return;
        }

        String serviceId = ExecutionIdUtil.getServiceId(executionId);

        String name = String.format(TENANT_FLOW_STASH_EXECUTION, serviceId, stashType, isSuccess ? "success" : "fail");
        ProfileUtil.accessStatistic(TENANT, name, System.currentTimeMillis(), executionTime);
        // 记录prometheus
        PrometheusUtil.statisticsTotalTime(PrometheusActions.METER_PREFIX + TENANT_STR + name, executionTime);
    }

    private void taskRunCount(String executionId, TaskInfo taskInfo) {
        try {
            String businessKey = buildBusinessKey(executionId);
            String taskCategory = taskInfo.getTask().getCategory();
            String serviceId = ExecutionIdUtil.getServiceId(executionId);

            String taskCountField = buildBusinessTaskCountField(serviceId, taskCategory);
            AtomicLong taskCountIncr = getIncrValue(Pair.of(businessKey, taskCountField));
            taskCountIncr.incrementAndGet();

            if (Objects.equals(taskCategory, TaskCategory.FUNCTION.getValue())) {
                String resourceName = ((FunctionTask) taskInfo.getTask()).getResourceName();
                businessResourceCount(resourceName, businessKey, serviceId);
            }
        } catch (Exception e) {
            log.warn("taskRunCount fails, executionId:{}, taskName:{}, errorMsg:{}",
                    executionId, Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null), e.getMessage());
        }
    }

    public void dagFinishCount(String executionId, DAGInfo dagInfo) {
        try {
            if (dagInfo == null) {
                log.warn("dagFinishCount can not get dagInfo, executionId:{}", executionId);
                return;
            }

            CallbackConfig callbackConfig = Optional.ofNullable(dagInfo.getDagInvokeMsg()).map(DAGInvokeMsg::getCallbackConfig)
                    .orElse(Optional.ofNullable(dagInfo.getDag()).map(DAG::getCallbackConfig).orElse(null));
            String resourceName = Optional.ofNullable(callbackConfig).map(CallbackConfig::getResourceName).orElse(null);
            if (StringUtils.isBlank(resourceName)) {
                return;
            }

            String businessKey = buildBusinessKey(executionId);
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            businessResourceCount(resourceName, businessKey, serviceId);
        } catch (Exception e) {
            log.warn("dagFinishCount fails, executionId:{}", executionId, e);
        }
    }

    public void dagSubmitCount(String executionId) {
        try {
            String businessKey = buildBusinessKey(executionId);
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String dagSubmitField = String.format("%s_dag_submit_count", serviceId);

            AtomicLong submitCountIncr = getIncrValue(Pair.of(businessKey, dagSubmitField));
            submitCountIncr.incrementAndGet();
        } catch (Exception e) {
            log.warn("dagSubmitCount fails, executionId:{}", executionId, e);
        }
    }

    public void finishNotifyCount(String executionId, NotifyInfo notifyInfo) {
        try {
            JSONObject context = Optional.ofNullable(notifyInfo)
                    .map(NotifyInfo::getTaskInvokeMsg)
                    .map(TaskInvokeMsg::getExt)
                    .map(it -> (JSONObject) it.get("context"))
                    .orElse(new JSONObject());
            long startTime = context.getLongValue("execute_time");
            long waitTime = startTime - context.getLongValue("submit_time");
            long executionTime = context.getLongValue("finish_time") - startTime;
            if (waitTime == 0L && executionTime == 0L) {
                return;
            }
            businessResourceTime(executionId, waitTime, executionTime);
            flowResourceTime(executionId, waitTime, executionTime);
        } catch (Exception e) {
            log.warn("finishNotifyCount fails, executionId:{}", executionId, e);
        }
    }

    public Map<String, String> getBusinessAggregate(String businessKey) {
        try {
            return businessAggregateClient.hgetAll(businessKey);
        } catch (Exception e) {
            log.warn("getBusinessAggregate fails, businessKey:{}", businessKey, e);
            return Collections.emptyMap();
        }
    }

    public Map<String, String> getFlowAggregate(String executionId) {
        try {
            String flowKey = buildFlowKey(executionId);
            return runtimeRedisClients.hgetAll(flowKey);
        } catch (Exception e) {
            log.warn("getBusinessAggregate fails, executionId:{}", executionId, e);
            return Collections.emptyMap();
        }
    }

    public void setBusinessValue() {
        try {
            MDC.put("request_id", UUID.randomUUID().toString());

            log.info("setBusinessValue action start");
            Map<String, Map<String, Long>> businessAggregate = Maps.newHashMap();
            fieldIncrValue.keySet().forEach(key -> Optional.ofNullable(fieldIncrValue.remove(key))
                    .map(AtomicLong::get)
                    .filter(it -> it > 0L)
                    .ifPresent(incrValue -> {
                        Map<String, Long> hash = businessAggregate.computeIfAbsent(key.getKey(), it -> Maps.newHashMap());
                        hash.put(key.getValue(), incrValue);
                    }));

            if (!switcherManagerImpl.getSwitcherState("ENABLE_TENANT_TASK_BUSINESS_AGGREGATE")) {
                log.info("setBusinessValue business aggregate switcher off, businessAggregate:{}", businessAggregate);
                return;
            }
            businessAggregateClient.pipelined().accept(pipeline -> {
                businessAggregate.forEach((businessKey, hash) -> {
                            hash.forEach((field, incrValue) -> pipeline.hincrBy(businessKey, field, incrValue));
                            pipeline.expire(businessKey, BUSINESS_AGGREGATE_EXPIRE_TIME_IN_SECOND);
                        }
                );
                pipeline.sync();
            });
        } catch (Exception e) {
            log.warn("setBusinessValue fails, ", e);
        }
    }

    private void businessResourceTime(String executionId, long waitTime, long executionTime) {
        String resourceType = "weibofunction";
        String businessKey = buildBusinessKey(executionId);
        String serviceId = ExecutionIdUtil.getServiceId(executionId);

        String waitTimeField = buildBusinessResourceWaitTimeField(serviceId, resourceType);
        AtomicLong waitTimeIncr = getIncrValue(Pair.of(businessKey, waitTimeField));
        waitTimeIncr.addAndGet(waitTime);

        String executeTimeField = buildBusinessResourceExecuteTimeField(serviceId, resourceType);
        AtomicLong executeTimeIncr = getIncrValue(Pair.of(businessKey, executeTimeField));
        executeTimeIncr.addAndGet(executionTime);
    }

    private void flowResourceTime(String executionId, long waitTime, long executionTime) {
        if (!switcherManagerImpl.getSwitcherState("ENABLE_TENANT_TASK_FLOW_AGGREGATE")) {
            log.info("flowResourceTime flow aggregate switcher off, executionId:{} waitTime:{} executionTime:{}", executionId, waitTime, executionTime);
            return;
        }

        String resourceType = "weibofunction";
        String flowKey = buildFlowKey(executionId);
        int reserveTime = ValueExtractor.getConfiguredValue(executionId, bizDConfs.getRedisBusinessIdToUnfinishedReserveSecond(), 86400);

        JedisFlowClient jedisFlowClient = (JedisFlowClient) runtimeRedisClients.choose(flowKey);
        jedisFlowClient.pipelined().accept(pipeline -> {
            if (waitTime > 0L) {
                pipeline.hincrBy(flowKey, buildFlowResourceWaitTimeField(resourceType), waitTime);
            }
            if (executionTime > 0L) {
                pipeline.hincrBy(flowKey, buildFlowResourceExecuteTimeField(resourceType), executionTime);
            }
            pipeline.expire(flowKey, reserveTime);
            pipeline.sync();
        });
    }

    private void businessResourceCount(String resourceName, String businessKey, String serviceId) {
        Resource resource = new Resource(resourceName);
        String resourceCountField = buildBusinessResourceCountField(serviceId, resource.getSchemeProtocol());
        getIncrValue(Pair.of(businessKey, resourceCountField)).incrementAndGet();
    }

    private AtomicLong getIncrValue(Pair<String, String> key) {
        return fieldIncrValue.computeIfAbsent(key, it -> new AtomicLong(0L));
    }

    private String buildBusinessKey(String executionId) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String time = dateFormat.format(new Date());
        return String.format(BUSINESS_AGGREGATE_KEY, ExecutionIdUtil.getBusinessId(executionId), time);
    }

    private String buildBusinessTaskCountField(String serviceId, String taskType) {
        return String.format(BUSINESS_AGGREGATE_TASK_COUNT_FIELD, serviceId, taskType);
    }

    private String buildBusinessResourceCountField(String serviceId, String resourceType) {
        return String.format(BUSINESS_AGGREGATE_RESOURCE_COUNT_FIELD, serviceId, resourceType);
    }

    private String buildBusinessResourceExecuteTimeField(String serviceId, String resourceType) {
        return String.format(BUSINESS_AGGREGATE_RESOURCE_EXECUTE_TIME_FIELD, serviceId, resourceType);
    }

    private String buildBusinessResourceWaitTimeField(String serviceId, String resourceType) {
        return String.format(BUSINESS_AGGREGATE_RESOURCE_WAIT_TIME_FIELD, serviceId, resourceType);
    }

    private String buildFlowKey(String executionId) {
        return String.format(FLOW_AGGREGATE_KEY, executionId);
    }

    private String buildFlowResourceExecuteTimeField(String resourceType) {
        return String.format(FLOW_AGGREGATE_RESOURCE_EXECUTE_TIME_FIELD, resourceType);
    }

    private String buildFlowResourceWaitTimeField(String resourceType) {
        return String.format(FLOW_AGGREGATE_RESOURCE_WAIT_TIME_FIELD, resourceType);
    }
}
