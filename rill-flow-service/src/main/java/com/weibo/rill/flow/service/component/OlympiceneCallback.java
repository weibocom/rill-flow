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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.service.util.PrometheusActions;
import com.weibo.rill.flow.olympicene.core.event.Callback;
import com.weibo.rill.flow.olympicene.core.event.Event;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.interfaces.model.task.InvokeTimeInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic;
import com.weibo.rill.flow.service.storage.LongTermStorage;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.service.util.ProfileActions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Slf4j
public class OlympiceneCallback implements Callback<DAGCallbackInfo> {
    private final HttpInvokeHelper httpInvokeHelper;
    private final JSONPathInputOutputMapping inputOutputMapping;
    private final LongTermStorage longTermStorage;
    private final ExecutorService callbackExecutor;
    private final TenantTaskStatistic tenantTaskStatistic;
    private final SwitcherManager switcherManagerImpl;

    public OlympiceneCallback(HttpInvokeHelper httpInvokeHelper,
                              JSONPathInputOutputMapping inputOutputMapping,
                              LongTermStorage longTermStorage,
                              ExecutorService callbackExecutor,
                              TenantTaskStatistic tenantTaskStatistic,
                              SwitcherManager switcherManagerImpl) {
        this.httpInvokeHelper = httpInvokeHelper;
        this.inputOutputMapping = inputOutputMapping;
        this.longTermStorage = longTermStorage;
        this.callbackExecutor = callbackExecutor;
        this.tenantTaskStatistic = tenantTaskStatistic;
        this.switcherManagerImpl = switcherManagerImpl;
    }

    @Override
    public void onEvent(Event<DAGCallbackInfo> event) {
        if (event == null || event.getData() == null) {
            return;
        }

        callbackExecutor.execute(() -> {
            int eventCode = event.getEventCode();
            DAGCallbackInfo eventData = event.getData();
            monitorLog(event.getId(), eventCode, eventData);
            if (eventCode == DAGEvent.DAG_SUCCEED.getCode() || eventCode == DAGEvent.DAG_FAILED.getCode()) {
                longTermStorage.storeDAGInfoAndContext(eventData);
                flowCompletedCallback(eventCode, eventData);
            }
        });
    }

    private void monitorLog(String executionId, int eventCode, DAGCallbackInfo eventData) {
        logCompleteEvent(executionId, eventCode, eventData);
        logTaskCode(executionId, eventCode, eventData);
    }
    
    private void logTaskCode(String executionId, int eventCode, DAGCallbackInfo eventData) {
        try {
            if (eventCode != DAGEvent.TASK_FAILED.getCode() &&
                    eventCode != DAGEvent.TASK_SKIPPED.getCode()) {
                return;
            }

            TaskInfo taskInfo = eventData.getTaskInfo();
            String code = Optional.ofNullable(taskInfo.getTaskInvokeMsg())
                    .map(TaskInvokeMsg::getCode).orElse(null);
            if (StringUtils.isNotBlank(code)) {
                ProfileActions.recordTaskCode(executionId, code, "total");
                String baseTaskName = DAGWalkHelper.getInstance().getBaseTaskName(taskInfo.getName());
                ProfileActions.recordTaskCode(executionId, code, baseTaskName);
                // 记录prometheus
                PrometheusActions.recordTaskCode(executionId, code, "total");
                PrometheusActions.recordTaskCode(executionId, code, baseTaskName);
            }
        } catch (Exception e) {
            log.warn("logTaskCode fails, eventCode:{}", eventCode, e);
        }
    }

    private void logCompleteEvent(String executionId, int eventCode, DAGCallbackInfo eventData) {
        try {
            if (eventCode > DAGEvent.DAG_SUCCEED.getCode()) {
                Optional.ofNullable(eventData.getTaskInfo())
                        .ifPresent(taskInfo -> {
                            long executionCost = getExecutionTime(taskInfo.getTaskInvokeMsg().getInvokeTimeInfos());
                            ProfileActions.recordTaskComplete(executionId, taskInfo.getTask().getCategory(), executionCost);
                            // 记录prometheus
                            PrometheusActions.recordTaskComplete(executionId, taskInfo.getTask().getCategory(), executionCost);
                            tenantTaskStatistic.taskProfileLog(executionCost, executionId, taskInfo.getName(), "complete");
                        });
            } else {
                long executionCost = getExecutionTime(eventData.getDagInfo().getDagInvokeMsg().getInvokeTimeInfos());
                ProfileActions.recordDAGComplete(executionId, executionCost);
                // 记录prometheus
                PrometheusActions.recordDAGComplete(executionId, executionCost);
            }
        } catch (Exception e) {
            log.warn("logCompleteEvent fails, eventCode:{}", eventCode, e);
        }
    }

    private long getExecutionTime(List<InvokeTimeInfo> invokeTimeInfos) {
        return Optional.ofNullable(invokeTimeInfos)
                .filter(CollectionUtils::isNotEmpty)
                .map(it -> it.get(it.size() - 1))
                .filter(it -> it.getStartTimeInMillisecond() != null && it.getEndTimeInMillisecond() != null)
                .map(it -> it.getEndTimeInMillisecond() - it.getStartTimeInMillisecond())
                .orElse(0L);
    }

    private void flowCompletedCallback(int eventCode, DAGCallbackInfo dagCallbackInfo) {
        try {
            DAGInfo dagInfo = dagCallbackInfo.getDagInfo();
            CallbackConfig callbackConfig = getCallbackConfig(dagInfo);

            String executionId = dagInfo.getExecutionId();
            String resourceName = Optional.ofNullable(callbackConfig).map(CallbackConfig::getResourceName).orElse(null);
            if (StringUtils.isBlank(resourceName)) {
                log.info("flowCompletedCallback return due to empty resourceName, executionId:{}", executionId);
                return;
            }

            HttpParameter requestParams = buildRequestParams(callbackConfig, dagCallbackInfo);
            String url = httpInvokeHelper.buildUrl(new Resource(resourceName), requestParams.getQueryParams());
            HttpHeaders httpHeaders = new HttpHeaders();
            if (requestParams.getHeader() != null) {
                requestParams.getHeader().forEach(httpHeaders::add);
            }
            httpInvokeHelper.appendRequestHeader(httpHeaders, executionId, null, dagCallbackInfo.getContext());
            HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestParams.getBody(), httpHeaders);
            int maxInvokeTime = switcherManagerImpl.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") ? 2 : 1;
            String taskInfoName = dagCallbackInfo.getTaskInfo() == null? null : dagCallbackInfo.getTaskInfo().getName();
            httpInvokeHelper.invokeRequest(executionId, taskInfoName, url, requestEntity, HttpMethod.POST, maxInvokeTime);
        } catch (Exception e) {
            log.warn("flowCompletedCallback fails, executionId:{}, eventCode:{}, errorMsg:{}",
                    dagCallbackInfo.getExecutionId(), eventCode, e.getMessage());
        }
    }

    private HttpParameter buildRequestParams(CallbackConfig callbackConfig, DAGCallbackInfo dagCallbackInfo) {
        DAGInfo dagInfo = dagCallbackInfo.getDagInfo();
        Map<String, Object> context = dagCallbackInfo.getContext();
        String executionId = dagInfo.getExecutionId();

        Map<String, Object> input = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(callbackConfig.getInputMappings())) {
            inputOutputMapping.mapping(context, input, new HashMap<>(), callbackConfig.getInputMappings());
        }
        HttpParameter httpParameter = httpInvokeHelper.buildRequestParams(executionId, input);

        Map<String, Object> body = httpParameter.getBody();
        body.put("execution_id", executionId);
        body.put("dag_status", dagInfo.getDagStatus().getValue());
        if (dagInfo.getDagStatus() == DAGStatus.FAILED) {
            Map<String, TaskInvokeMsg> failedTaskInvokeMsgMap = DAGWalkHelper.getInstance().getFailedTasks(dagInfo).stream()
                    .collect(Collectors.toMap(TaskInfo::getName, TaskInfo::getTaskInvokeMsg));
            body.put("error_info", ImmutableMap.of(
                    "dag_invoke_info", dagInfo.getDagInvokeMsg(),
                    "failed_tasks_invoke_info", failedTaskInvokeMsgMap));
        }
        if (Optional.ofNullable(callbackConfig.getFullDAGInfo()).orElse(false)) {
            body.put("dag_info", dagInfo);
        }
        if (Optional.ofNullable(callbackConfig.getFullContext()).orElse(false)) {
            body.put("data", context);
        }

        log.info("request body: {}", body);

        return httpParameter;
    }

    private CallbackConfig getCallbackConfig(DAGInfo dagInfo) {
        CallbackConfig defaultCallback = Optional.ofNullable(dagInfo.getDag())
                .map(DAG::getCallbackConfig)
                .orElse(null);
        return Optional.ofNullable(dagInfo.getDagInvokeMsg())
                .map(DAGInvokeMsg::getCallbackConfig)
                .orElse(defaultCallback);
    }
}