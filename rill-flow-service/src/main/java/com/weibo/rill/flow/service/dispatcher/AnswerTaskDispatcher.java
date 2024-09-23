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

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.olympicene.core.model.task.AnswerTask;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.annotation.Resource;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class AnswerTaskDispatcher implements DAGDispatcher {
    @Value("${rill.flow.sse.executor.answer.execute.uri}")
    private String answerTaskExecuteUri;
    @Value("${rill.flow.sse.executor.host}")
    private String sseExecutorHost;

    @Resource
    private HttpInvokeHelper httpInvokeHelper;
    @Resource
    private SwitcherManager switcherManagerImpl;

    @Override
    public String dispatch(DispatchInfo dispatchInfo) {
        AnswerTask answerTask = (AnswerTask) dispatchInfo.getTaskInfo().getTask();
        if (answerTask == null || StringUtils.isEmpty(answerTask.getExpression())) {
            return new JSONObject(Map.of("data", "failed", "message", "task or expression cannot be null")).toJSONString();
        }
        String executionId = dispatchInfo.getExecutionId();
        String taskName = answerTask.getName();
        String expression = answerTask.getExpression();

        try {
            String url = buildUrl(executionId, taskName);

            HttpEntity<?> requestEntity = buildHttpEntity(dispatchInfo, expression);
            int maxInvokeTime = switcherManagerImpl.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") ? 2 : 1;
            return httpInvokeHelper.invokeRequest(executionId, taskName, url, requestEntity, HttpMethod.POST, maxInvokeTime);
        } catch (URISyntaxException e) {
            log.warn("dispatch answer task error, execution_id: {}, task_name: {}, expression: {}", executionId, taskName, expression, e);
            return new JSONObject(Map.of("data", "failed", "message", e.getMessage())).toJSONString();
        }
    }

    @NotNull
    private String buildUrl(String executionId, String taskName) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(sseExecutorHost + answerTaskExecuteUri);
        uriBuilder.addParameter("execution_id", executionId);
        uriBuilder.addParameter("task_name", taskName);
        return uriBuilder.toString();
    }

    private static HttpEntity<?> buildHttpEntity(DispatchInfo dispatchInfo, String expression) {
        Map<String, Object> body = Map.of("expression", expression);
        MultiValueMap<String, String> header = dispatchInfo.getHeaders();
        if (header == null) {
            header = new LinkedMultiValueMap<>();
        }
        header.put(HttpHeaders.CONTENT_TYPE, List.of(MediaType.APPLICATION_JSON_VALUE));
        header.put("X-Mode", List.of("sync"));
        return new HttpEntity<>(body, header);
    }
}
