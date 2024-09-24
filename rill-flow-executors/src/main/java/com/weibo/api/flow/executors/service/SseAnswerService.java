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
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.weibo.api.flow.executors.model.constant.SseConstant;
import com.weibo.api.flow.executors.model.enums.AnswerExpressionMetaType;
import com.weibo.api.flow.executors.model.enums.DAGStatus;
import com.weibo.api.flow.executors.model.enums.TaskStatus;
import com.weibo.api.flow.executors.model.event.SseEvent;
import com.weibo.api.flow.executors.model.params.SseAnswerDispatchParams;
import com.weibo.api.flow.executors.model.tasks.AnswerExpressionMeta;
import com.weibo.api.flow.executors.redis.RedisClient;
import com.weibo.api.flow.executors.utils.ExpressionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class SseAnswerService {
    @Resource
    private RedisClient sseExecutorRedisClient;
    @Resource
    private RillFlowService rillFlowService;
    @Resource
    private SseExecutorService sseExecutorService;
    @Resource
    private ThreadPoolExecutor sseThreadPoolExecutor;
    @Resource
    private ApplicationEventPublisher applicationEventPublisher;

    private static final String ANSWER_TASK_NAMES_PREFIX = "answer_task-";
    private static final String ANSWER_TASK_EXPRESSION_PREFIX = "answer_expression-";
    private static final Configuration JSONPATH_CONF = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();

    public void execute(String executionId, String answerTaskName, SseAnswerDispatchParams params) {
        if (params == null || StringUtils.isEmpty(params.getExpression())) {
            return;
        }
        String answerTaskNamesKey = ANSWER_TASK_NAMES_PREFIX + executionId;
        String answerTaskExpressionsKey = ANSWER_TASK_EXPRESSION_PREFIX + executionId;
        try {
            sseExecutorRedisClient.lpush(answerTaskNamesKey, answerTaskName);
            sseExecutorRedisClient.hset(answerTaskExpressionsKey, answerTaskName, params.getExpression());
        } catch (Exception e) {
            log.error("sse answer execute error, execution_id: {}, answer_task_name: {}, expression: {}",
                    executionId, answerTaskName, params.getExpression(), e);
        } finally {
            sseExecutorRedisClient.expire(answerTaskNamesKey, SseConstant.REDIS_EXPIRE);
            sseExecutorRedisClient.expire(answerTaskExpressionsKey, SseConstant.REDIS_EXPIRE);
        }
    }

    public void sseSendAnswerTaskResult(String rillFlowHost, String executionId, String answerTaskName, SseEmitter emitter, boolean quiet) throws Exception {
        while (true) {
            boolean isRunning = rillFlowService.isTaskRunning(rillFlowHost, executionId, answerTaskName);
            if (isRunning) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(SseConstant.SSE_DATA_GET_INTERVAL);
        }
        applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                .event("answer_task_started")
                .executionId(executionId)
                .answerTaskName(answerTaskName)
                .emitter(emitter)
                .quiet(quiet)
                .build()));
        String expression = sseExecutorRedisClient.hget(ANSWER_TASK_EXPRESSION_PREFIX + executionId, answerTaskName);
        if (expression == null) {
            applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                    .event("answer_task_error")
                    .executionId(executionId)
                    .answerTaskName(answerTaskName)
                    .message("expression is null")
                    .emitter(emitter)
                    .quiet(quiet)
                    .build()));
            throw new IllegalArgumentException("expression is null, execution_id: " + executionId + ", task_name: " + answerTaskName);
        }
        List<AnswerExpressionMeta> answerExpressionMetas = ExpressionUtil.parseAnswerTaskExpression(expression);
        answerExpressionMetas.forEach(answerExpressionMeta -> {
            try {
                switch (answerExpressionMeta.getType()) {
                    case PLAIN -> applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                            .event("message")
                            .executionId(executionId)
                            .answerTaskName(answerTaskName)
                            .message(answerExpressionMeta.getText())
                            .type(AnswerExpressionMetaType.PLAIN)
                            .emitter(emitter)
                            .quiet(quiet)
                            .build()));
                    case FUNCTION_TASK -> sendNormalTaskResult(rillFlowHost, executionId, answerTaskName, answerExpressionMeta, emitter, quiet);
                    case STREAM_TASK -> sendStreamTaskResult(rillFlowHost, executionId, answerTaskName, answerExpressionMeta, emitter, quiet);
                }
            } catch (Exception e) {
                log.warn("emitter send error, execution_id: {}, task_name: {}, expression meta: {}", executionId, answerTaskName, answerExpressionMeta, e);
            }
        });
    }

    private void sendNormalTaskResult(String rillFlowHost, String executionId, String answerTaskName,
                                      AnswerExpressionMeta answerExpressionMeta, SseEmitter emitter, boolean quiet) {
        String functionTaskName = answerExpressionMeta.getTaskName();
        try {
            while (true) {
                TaskStatus taskStatus = rillFlowService.getTaskStatus(rillFlowHost, executionId, functionTaskName);
                if (taskStatus == null || taskStatus.ordinal() < TaskStatus.SUCCEED.ordinal()) {
                    TimeUnit.MILLISECONDS.sleep(SseConstant.SSE_DATA_GET_INTERVAL);
                    continue;
                }
                if (taskStatus.isSuccessOrSkip()) {
                    break;
                }
                if (taskStatus.isFailed()) {
                    log.warn("task run failed: {}", functionTaskName);
                    throw new IllegalArgumentException("task run failed, " + functionTaskName);
                }
            }
            applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                    .event("normal_task_finished")
                    .executionId(executionId)
                    .taskName(functionTaskName)
                    .quiet(quiet)
                    .emitter(emitter)
                    .build()));
            JSONObject context = rillFlowService.getContext(rillFlowHost, executionId);
            Object result = JsonPath.using(JSONPATH_CONF).parse(Map.of("context", context.getJSONObject("ret"))).read(answerExpressionMeta.getText());
            if (result != null) {
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("message")
                        .executionId(executionId)
                        .answerTaskName(answerTaskName)
                        .message(String.valueOf(result))
                        .type(AnswerExpressionMetaType.FUNCTION_TASK)
                        .emitter(emitter)
                        .quiet(quiet)
                        .build()));
            }
        } catch (Exception e) {
            applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                    .event("normal_task_error")
                    .executionId(executionId)
                    .taskName(functionTaskName)
                    .message("send normal task result error")
                    .exception(e)
                    .emitter(emitter)
                    .quiet(quiet)
                    .build()));
        }
    }

    private void sendStreamTaskResult(String rillFlowHost, String executionId, String answerTaskName,
                                      AnswerExpressionMeta answerExpressionMeta, SseEmitter emitter, boolean quiet)
            throws Exception {
        String sseTaskName = answerExpressionMeta.getTaskName();
        sseExecutorService.getSseTaskResult(rillFlowHost, executionId, answerTaskName, sseTaskName, emitter, quiet);
    }

    public void sseSendDagResult(String rillFlowHost, String executionId, SseEmitter emitter, boolean quiet) throws Exception {
        Set<String> handledAnswerTaskNames = new HashSet<>();
        String answerTaskNamesKey = ANSWER_TASK_NAMES_PREFIX + executionId;
        boolean dagComplete = false;
        int index = 0;
        while (!dagComplete) {
            DAGStatus dagStatus = rillFlowService.getDAGStatus(rillFlowHost, executionId);
            if (dagStatus == null || dagStatus == DAGStatus.FAILED) {
                log.warn("dagStatus get failed, execution_id: {}", executionId);
                break;
            }
            if (dagStatus == DAGStatus.SUCCEED) {
                dagComplete = true;
            }
            List<String> answerTaskNames = sseExecutorRedisClient.lrange(answerTaskNamesKey, index, -1);
            if (answerTaskNames == null || answerTaskNames.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(SseConstant.SSE_DATA_GET_INTERVAL);
                continue;
            }
            for (String answerTaskName : answerTaskNames) {
                index++;
                if (handledAnswerTaskNames.contains(answerTaskName)) {
                    continue;
                }
                sseSendAnswerTaskResult(rillFlowHost, executionId, answerTaskName, emitter, quiet);
                handledAnswerTaskNames.add(answerTaskName);
            }
            TimeUnit.MILLISECONDS.sleep(SseConstant.SSE_DATA_GET_INTERVAL);
        }
    }

    public SseEmitter submitForDagResult(String rillFlowHost, String descriptorId, JSONObject context, boolean quiet) {
        String executionId = rillFlowService.submitFlow(rillFlowHost, descriptorId, context);
        SseEmitter emitter = new SseEmitter(SseConstant.SSE_EMITTER_TIMEOUT);
        sseThreadPoolExecutor.submit(() -> {
            try {
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("workflow_started")
                        .executionId(executionId)
                        .emitter(emitter)
                        .quiet(quiet)
                        .build()));
                sseSendDagResult(rillFlowHost, executionId, emitter, quiet);
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("workflow_finished")
                        .executionId(executionId)
                        .emitter(emitter)
                        .completed(true)
                        .quiet(quiet)
                        .build()));
            } catch (Exception e) {
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("workflow_execute_error")
                        .executionId(executionId)
                        .exception(e)
                        .completed(true)
                        .quiet(quiet)
                        .emitter(emitter)
                        .build()));
                log.warn("send execution_id error, execution_id: {}", executionId, e);
            }
        });
        return emitter;
    }
}
