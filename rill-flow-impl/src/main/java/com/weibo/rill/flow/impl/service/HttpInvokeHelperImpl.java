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

package com.weibo.rill.flow.impl.service;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.task.FunctionPattern;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.service.auth.AuthHeaderGenerator;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
public class HttpInvokeHelperImpl implements HttpInvokeHelper {
    @Autowired
    @Qualifier("rillFlowHttpTemplate")
    private RestTemplate defaultRestTemplate;
    @Autowired
    @Qualifier("authHeaderGenerator")
    private AuthHeaderGenerator authHeaderGenerator;

    private final Logger httpAccessLogger = LoggerFactory.getLogger("httpclientaccess");

    @Override
    public void appendRequestHeader(HttpHeaders httpHeaders, String executionId, TaskInfo task, Map<String, Object> input) {
        authHeaderGenerator.appendRequestHeader(httpHeaders, executionId, task, input);
        if (task != null && task.getTask() instanceof FunctionTask functionTask) {
            if (FunctionPattern.TASK_SCHEDULER.equals(functionTask.getPattern())
                    || FunctionPattern.TASK_ASYNC.equals(functionTask.getPattern())) {
                httpHeaders.add("X-Mode", "async");
            }
        }
    }

    @Override
    public HttpParameter functionRequestParams(String executionId, String taskInfoName, Resource resource, Map<String, Object> input) {
        HttpParameter httpParameter = buildRequestParams(executionId, input);
        Map<String, Object> queryParams = httpParameter.getQueryParams();
        queryParams.put("name", taskInfoName);
        log.info("buildRequestParams result queryParams:{} body:{}, executionId:{}， taskInfoName:{}", queryParams, httpParameter.getBody(), executionId, taskInfoName);
        return httpParameter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public HttpParameter buildRequestParams(String executionId, Map<String, Object> input) {

        Map<String, Object> queryParams = Maps.newHashMap();
        Map<String, Object> body = Maps.newHashMap();
        Map<String, Object> callback = Maps.newHashMap();
        Map<String, String> header = Maps.newHashMap();

        queryParams.put("execution_id", executionId);
        body.put("execution_id", executionId);
        Optional.ofNullable(input).ifPresent(inputMap -> inputMap.forEach((key, value) -> {
            if (!(value instanceof Map)) {
                body.put(key, value);
            } else if (key.startsWith("request_config_") || key.equals("body")) {
                body.putAll((Map<String, Object>) value);
            } else if (key.startsWith("query_params_") || key.equals("query")) {
                queryParams.putAll((Map<String, Object>) value);
            } else if (key.startsWith("request_callback_")) {
                callback.putAll((Map<String, Object>) value);
            } else if (key.startsWith("request_header_") || key.equals("header")) {
                header.putAll((Map<String, String>) value);
            } else {
                body.put(key, value);
            }
        }));

        return HttpParameter.builder().header(header).queryParams(queryParams).body(body).callback(callback).build();
    }

    @Override
    public String buildUrl(Resource resource, Map<String, Object> queryParams) {
        String originalUrl;
        if (resource.isHttpResource()) {
            originalUrl = resource.getResourceName();
        } else {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "scheme protocol:" + resource.getSchemeProtocol() + " unsupported");
        }

        try {
            URIBuilder uriBuilder = new URIBuilder(originalUrl);
            Optional.ofNullable(queryParams).ifPresent(params ->
                    params.forEach((key, value) -> uriBuilder.addParameter(key, String.valueOf(value))));
            String url = uriBuilder.toString();
            log.info("buildUrl url:{}", url);
            return url;
        } catch (Exception e) {
            log.warn("buildUrl fails, originalUrl:{}, resource:{}, queryParams:{}", originalUrl, resource, queryParams, e);
            throw new TaskException(BizError.ERROR_URI, "originalUrl value:" + originalUrl + " not http format");
        }
    }

    @Override
    public String invokeRequest(String executionId, String taskInfoName, String url, HttpEntity<?> requestEntity, HttpMethod method, int maxInvokeTime) {
        RestTemplate restTemplate = defaultRestTemplate;
        String cause = null;
        for (int i = 1; i <= maxInvokeTime; i++) {
            long startTime = System.currentTimeMillis();
            ResponseEntity<String> responseEntity = null;
            try {
                if (method == HttpMethod.GET) {
                    responseEntity = restTemplate.exchange(new URI(url), method, requestEntity, String.class);
                } else {
                    responseEntity = restTemplate.postForEntity(new URI(url), requestEntity, String.class);
                }
                return responseEntity.getBody();
            } catch (Exception e) {
                cause = e.getMessage();
            } finally {
                postHttpProcess(url, requestEntity, method, System.currentTimeMillis() - startTime, responseEntity);
            }
        }
        throw new TaskException(BizError.ERROR_INVOKE_URI.getCode(), String.format("dispatchTask http fails due to %s", cause));
    }


    private void postHttpProcess(String url, HttpEntity<?> requestEntity, HttpMethod method, long timeout, ResponseEntity<String> responseEntity) {
        try {
            int code = responseEntity == null ? 500 : responseEntity.getStatusCode().value();
            // 打印 http_access 日志
            Object requestBody = parseBodyForHttpAccessLog(requestEntity.getBody());
            String responseBody = responseEntity == null ? "" : responseEntity.getBody();
            if (responseBody == null) {
                responseBody = "";
            }
            int responseLength = responseBody.length();
            responseBody = StringUtils.substring(responseBody, 0, 1000);
            httpAccessLogger.info("{} {} {} {} {} {} {}", timeout, method, code, responseLength, url, requestBody, responseBody);
        } catch (Exception e) {
            log.warn("httpAccess log error", e);
        }
    }

    private Object parseBodyForHttpAccessLog(Object body) {
        try {
            if (body instanceof Map) {
                return new JSONObject((Map<String, Object>) body).toJSONString();
            }
        } catch (Exception e) {
            log.warn("parseBodyForHttpAccessLog error: ", e);
        }
        return body;
    }
}
