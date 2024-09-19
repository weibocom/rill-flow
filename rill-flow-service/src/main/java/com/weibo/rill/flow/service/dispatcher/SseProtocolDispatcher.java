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

package com.weibo.rill.flow.service.dispatcher;

import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic;
import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientResponseException;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service("sseDispatcher")
public class SseProtocolDispatcher implements DispatcherExtension {
    @Value("${rill.flow.sse.executor.execute.uri}")
    private String sseExecutorUri;
    @Value("${rill.flow.sse.executor.host}")
    private String sseExecutorHost;

    @Autowired
    private HttpInvokeHelper httpInvokeHelper;
    @Autowired
    private DAGResourceStatistic dagResourceStatistic;

    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        Map<String, Object> input = dispatchInfo.getInput();
        TaskInfo taskInfo = dispatchInfo.getTaskInfo();
        String executionId = dispatchInfo.getExecutionId();
        String taskInfoName = taskInfo.getName();
        String requestType = ((FunctionTask) taskInfo.getTask()).getRequestType();
        MultiValueMap<String, String> header = dispatchInfo.getHeaders();
        String result = null;

        try {
            HttpEntity<?> requestEntity = buildHttpEntity(executionId, taskInfoName, resource, header, requestType, input);
            String url = buildUrl(executionId, taskInfoName);
            result = httpInvokeHelper.invokeRequest(executionId, taskInfoName, url, requestEntity, HttpMethod.POST, 1);
        } catch (RestClientResponseException e) {
            result = e.getResponseBodyAsString();
            throw new TaskException(BizError.ERROR_INVOKE_URI.getCode(),
                    String.format("dispatchTask sse fails status code: %s text: %s", e.getRawStatusCode(), result));
        } catch (Exception e) {
            result = e.getMessage();
            throw new TaskException(BizError.ERROR_INTERNAL.getCode(), "dispatchTask sse fails: " + e.getMessage());
        } finally {
            dagResourceStatistic.updateUrlTypeResourceStatus(executionId, taskInfoName, resource.getResourceName(), result);
        }
        return result;
    }

    @NotNull
    private String buildUrl(String executionId, String taskInfoName) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(sseExecutorHost + sseExecutorUri);
        uriBuilder.addParameter("execution_id", executionId);
        uriBuilder.addParameter("task_name", taskInfoName);
        return uriBuilder.toString();
    }

    @NotNull
    private HttpEntity<?> buildHttpEntity(String executionId, String taskInfoName, Resource resource,
                                          MultiValueMap<String, String> header, String requestType, Map<String, Object> input) {
        HttpParameter requestParams = httpInvokeHelper.functionRequestParams(executionId, taskInfoName, resource, input);
        Map<String, Object> body = new HashMap<>();
        String url = httpInvokeHelper.buildUrl(resource, requestParams.getQueryParams());
        body.put("url", url);
        body.put("body", requestParams.getBody());
        if (requestParams.getBody().get("callback_info") != null) {
            body.put("callback_info", requestParams.getBody().get("callback_info"));
        } else if (header.getFirst("X-Callback-Url") != null) {
            body.put("callback_info", Map.of("trigger_url", header.getFirst("X-Callback-Url")));
        } else {
            throw new TaskException(BizError.ERROR_INTERNAL, "cannot find callback url");
        }
        body.put("headers", requestParams.getHeader());
        body.put("request_type", Optional.ofNullable(requestType).map(String::toUpperCase).orElse("GET"));
        header.putIfAbsent(HttpHeaders.CONTENT_TYPE, List.of(MediaType.APPLICATION_JSON_VALUE));
        return new HttpEntity<>(body, header);
    }

    @Override
    public String getName() {
        return "sse";
    }
}
