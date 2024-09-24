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

package com.weibo.api.flow.executors.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weibo.api.flow.executors.model.enums.DAGStatus;
import com.weibo.api.flow.executors.model.enums.TaskStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class RillFlowService {
    @Value("${rill.flow.task.check.uri:/flow/get.json}")
    private String rillFlowExecutionInfoGetUri;
    @Value("${rill.flow.task.check.uri:/flow/submit.json}")
    private String rillFlowSubmitUri;
    @Value("${rill.flow.get.context.uri:/flow/get_context.json}")
    private String rillFlowGetContextUri;

    @Resource
    private RestTemplate rillFlowHttpTemplate;


    public TaskStatus getTaskStatus(String rillFlowHost, String executionId, String taskName) {
        JSONObject responseJson = getExecutionInfo(rillFlowHost, executionId);
        if (responseJson == null)
            return null;
        String dagStatus = Optional.of(responseJson).map(it -> it.getJSONObject("ret")).map(it -> it.getString("dag_status")).orElse(null);
        if ("FAILED".equalsIgnoreCase(dagStatus)) {
            return TaskStatus.FAILED;
        }
        String status = Optional.of(responseJson).map(it -> it.getJSONObject("ret"))
                .map(it -> it.getJSONObject("tasks")).map(it -> it.getJSONObject(taskName))
                .map(it -> it.getString("status")).orElse(null);
        if (status == null) {
            throw new IllegalArgumentException("task not exist");
        }
        return TaskStatus.valueOf(status);
    }

    @Nullable
    private JSONObject getExecutionInfo(String rillFlowHost, String executionId) {
        JSONObject responseJson;
        try {
            MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
            URIBuilder uriBuilder = new URIBuilder(rillFlowHost + rillFlowExecutionInfoGetUri);
            uriBuilder.addParameter("execution_id", executionId);
            header.put(HttpHeaders.CONTENT_TYPE, List.of(MediaType.APPLICATION_JSON_VALUE));
            HttpEntity<Object> requestEntity = new HttpEntity<>(null, header);
            ResponseEntity<String> response = rillFlowHttpTemplate.exchange(uriBuilder.build().toString(), HttpMethod.GET, requestEntity, String.class);
            responseJson = JSON.parseObject(response.getBody());
        } catch (Exception e) {
            log.error("getTaskStatus error, execution_id: {}, error: ", executionId, e);
            return null;
        }
        return responseJson;
    }

    public JSONObject getContext(String rillFlowHost, String executionId) {
        try {
            MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
            header.put(HttpHeaders.CONTENT_TYPE, List.of(MediaType.APPLICATION_JSON_VALUE));
            URIBuilder uriBuilder = new URIBuilder(rillFlowHost + rillFlowGetContextUri);
            uriBuilder.addParameter("execution_id", executionId);
            HttpEntity<Object> requestEntity = new HttpEntity<>(null, header);
            ResponseEntity<String> response = rillFlowHttpTemplate.exchange(uriBuilder.build().toString(), HttpMethod.GET, requestEntity, String.class);
            return JSON.parseObject(response.getBody());
        } catch (Exception e) {
            log.error("getContext error, execution_id: {}, error: ", executionId, e);
            return null;
        }
    }

    public void callbackResult(JSONObject callbackInfo, boolean success, List<String> result) {
        if (callbackInfo == null || callbackInfo.getString("trigger_url") == null) {
            return;
        }
        String callbackUrl = String.format("%s&status=%s", callbackInfo.getString("trigger_url"), success ? "success": "failed");
        MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
        header.put(HttpHeaders.CONTENT_TYPE, List.of(MediaType.APPLICATION_JSON_VALUE));
        JSONObject body = new JSONObject(Map.of("data", result));
        HttpEntity<Object> requestEntity = new HttpEntity<>(body, header);
        rillFlowHttpTemplate.exchange(callbackUrl, HttpMethod.GET, requestEntity, String.class);
    }

    public String submitFlow(String rillFlowHost, String descriptorId, JSONObject context) {
        MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
        header.put(HttpHeaders.CONTENT_TYPE, List.of(MediaType.APPLICATION_JSON_VALUE));
        // Adding a CSRF token to the header to prevent CSRF attacks
        String csrfToken = generateCsrfToken();
        header.put("X-CSRF-Token", List.of(csrfToken));
        HttpEntity<Object> requestEntity = new HttpEntity<>(context, header);
        try {
            URIBuilder uriBuilder = new URIBuilder(rillFlowHost + rillFlowSubmitUri);
            uriBuilder.addParameter("descriptor_id", descriptorId);
            ResponseEntity<String> response = rillFlowHttpTemplate.exchange(uriBuilder.build().toString(), HttpMethod.POST, requestEntity, String.class);
            JSONObject responseObj = JSON.parseObject(response.getBody());
            return responseObj.getString("execution_id");
        } catch (Exception e) {
            log.error("submitFlow error, descriptor_id: {}, error: ", descriptorId, e);
            return null;
        }
    }

    private String generateCsrfToken() {
        // Implement a method to generate a CSRF token
        return UUID.randomUUID().toString();
    }

    public boolean isTaskRunning(String rillFlowHost, String executionId, String taskName) {
        TaskStatus taskStatus = getTaskStatus(rillFlowHost, executionId, taskName);
        if (taskStatus != null && taskStatus.ordinal() >= TaskStatus.FAILED.ordinal()) {
            throw new IllegalArgumentException("task run failed or be skipped");
        } else return taskStatus == null || taskStatus.ordinal() >= TaskStatus.RUNNING.ordinal();
    }

    public DAGStatus getDAGStatus(String rillFlowHost, String executionId) {
        JSONObject responseJson = getExecutionInfo(rillFlowHost, executionId);
        if (responseJson == null) {
            return null;
        }
        String dagStatus = Optional.of(responseJson).map(it -> it.getJSONObject("ret")).map(it -> it.getString("dag_status")).orElse(null);
        return DAGStatus.parse(dagStatus);
    }
}
