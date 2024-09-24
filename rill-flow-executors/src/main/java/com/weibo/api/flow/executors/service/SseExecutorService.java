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

import com.alibaba.fastjson.JSONObject;
import com.weibo.api.flow.executors.model.constant.SseConstant;
import com.weibo.api.flow.executors.model.enums.AnswerExpressionMetaType;
import com.weibo.api.flow.executors.model.event.SseEvent;
import com.weibo.api.flow.executors.model.params.SseDispatchParams;
import com.weibo.api.flow.executors.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class SseExecutorService {

    @Resource
    private RestTemplate rillFlowSSEHttpTemplate;
    @Resource
    private RedisClient sseExecutorRedisClient;
    @Resource
    private ThreadPoolExecutor sseThreadPoolExecutor;
    @Resource
    private RillFlowService rillFlowService;
    @Resource
    private ApplicationEventPublisher applicationEventPublisher;

    private static final String REDIS_KEY_PREFIX = "sse-executor-";
    private static final String REDIS_MAX_INDEX_PREFIX = "sse-max-index-";

    public void getSseTaskResult(String rillFlowHost, String executionId, String answerTaskName, String taskName, SseEmitter emitter, boolean quiet) throws Exception {
        double maxIndex = 0.0;
        boolean isComplete = false;
        while (true) {
            boolean isRunning = rillFlowService.isTaskRunning(rillFlowHost, executionId, taskName);
            if (!isRunning) {
                TimeUnit.MILLISECONDS.sleep(SseConstant.SSE_DATA_GET_INTERVAL);
            } else {
                break;
            }
        }

        applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                .event("sse_task_started")
                .executionId(executionId)
                .answerTaskName(answerTaskName)
                .taskName(taskName)
                .emitter(emitter)
                .quiet(quiet)
                .build()));

        while (!isComplete) {
            Set<Pair<String, Double>> wordSet = getWordTreeSetFromRedis(executionId, taskName, maxIndex);
            for (Pair<String, Double> pair : wordSet) {
                if (pair.getValue() > maxIndex) {
                    maxIndex = pair.getValue();
                }
                if (pair.getKey().equals(executionId + "#" + taskName + "-completed")) {
                    isComplete = true;
                    break;
                }
                String data = pair.getKey().substring(pair.getKey().indexOf("_") + 1);
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("message")
                        .executionId(executionId)
                        .answerTaskName(answerTaskName)
                        .taskName(taskName)
                        .emitter(emitter)
                        .quiet(quiet)
                        .type(AnswerExpressionMetaType.STREAM_TASK)
                        .message(data).build()));
            }
            TimeUnit.MILLISECONDS.sleep(SseConstant.SSE_DATA_GET_INTERVAL);
        }

        applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                .event("sse_task_finished")
                .executionId(executionId)
                .answerTaskName(answerTaskName)
                .taskName(taskName)
                .emitter(emitter)
                .quiet(quiet)
                .build()));
    }

    private Set<Pair<String, Double>> getWordTreeSetFromRedis(String executionId, String taskName, double index) {
        String redisKey = REDIS_KEY_PREFIX + executionId + "#" + taskName;
        Set<Pair<String, Double>> wordSet = sseExecutorRedisClient.zrangeWithScores(redisKey, redisKey, (int) index, -1);
        if (CollectionUtils.isEmpty(wordSet)) {
            return new HashSet<>();
        }
        Set<Pair<String, Double>> wordTreeSet = new TreeSet<>(Map.Entry.comparingByValue());
        wordTreeSet.addAll(wordSet);
        return wordTreeSet;
    }

    public void callSSETask(String executionId, String taskName, SseDispatchParams params) {
        boolean success = true;
        List<String> result = null;
        try {
            HttpMethod method = HttpMethod.resolve(params.getRequestType());
            if (method == null) {
                method = HttpMethod.GET;
            }
            Map<String, Object> body = null;
            if (method == HttpMethod.POST) {
                body = params.getBody();
            }
            MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
            if (params.getHeaders() != null) {
                params.getHeaders().forEach((key, value) -> headers.put(key, List.of(String.valueOf(value))));
            }
            HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);
            RequestCallback requestCallback = rillFlowSSEHttpTemplate.httpEntityCallback(requestEntity, String.class);
            result = rillFlowSSEHttpTemplate.execute(params.getUrl(), method, requestCallback,
                    response -> handleSseTaskResponse(executionId, taskName, response));
        } catch (Exception e) {
            log.warn("Error while executing sse, execution_id: {}, task_name: {}: ", executionId, taskName, e);
            success = false;
        } finally {
            String redisKey = executionId + "#" + taskName;
            long index = sseExecutorRedisClient.incr(REDIS_MAX_INDEX_PREFIX + redisKey);
            sseExecutorRedisClient.zadd(REDIS_KEY_PREFIX + redisKey, index, redisKey + "-completed");
            sseExecutorRedisClient.expire(REDIS_KEY_PREFIX + redisKey, SseConstant.REDIS_EXPIRE);
            sseExecutorRedisClient.expire(REDIS_MAX_INDEX_PREFIX + redisKey, SseConstant.REDIS_EXPIRE);
            rillFlowService.callbackResult(params.getCallbackInfo(), success, result);
        }
    }

    private List<String> handleSseTaskResponse(String executionId, String taskName, ClientHttpResponse response) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.getBody()));
        String data;
        String redisKey = executionId + "#" + taskName;
        List<String> result = new ArrayList<>();
        try {
            while ((data = bufferedReader.readLine()) != null) {
                if (data.startsWith(SseConstant.SSE_MARK)) {
                    data = data.substring(SseConstant.SSE_MARK.length());
                } else {
                    continue;
                }
                result.add(data);
                long index = sseExecutorRedisClient.incr(REDIS_MAX_INDEX_PREFIX + redisKey);
                long redisResult = sseExecutorRedisClient.zadd(REDIS_KEY_PREFIX + redisKey, index, index + "_" + data);
                log.info("got sse data: {}, execution_id: {}, task_name: {}, redis result: {}", data, executionId, taskName, redisResult);
            }
        } catch (IOException e) {
            log.warn("Error while reading, execution_id: {}, task_name: {}, data: ", executionId, taskName, e);
        }
        return result;
    }

    public SseEmitter submitForResult(String rillFlowHost, String descriptorId, String taskName, JSONObject context, boolean quiet) {
        String executionId = rillFlowService.submitFlow(rillFlowHost, descriptorId, context);
        SseEmitter sseEmitter = new SseEmitter(SseConstant.SSE_EMITTER_TIMEOUT);
        sseThreadPoolExecutor.submit(() -> {
            try {
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("workflow_started")
                        .executionId(executionId)
                        .quiet(quiet)
                        .build()));
                getSseTaskResult(rillFlowHost, executionId, null, taskName, sseEmitter, quiet);
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("workflow_finished")
                        .executionId(executionId)
                        .completed(true)
                        .quiet(quiet)
                        .build()));
            } catch (Exception e) {
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("workflow_error")
                        .executionId(executionId)
                        .exception(e)
                        .completed(true)
                        .quiet(quiet)
                        .build()));
                log.warn("send execution_id error, execution_id: {}", executionId, e);
            }
        });
        return sseEmitter;
    }
}
