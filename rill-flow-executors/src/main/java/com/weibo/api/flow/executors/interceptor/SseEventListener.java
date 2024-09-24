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

package com.weibo.api.flow.executors.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.weibo.api.flow.executors.model.event.SseEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.LinkedHashMap;

@Service
@Slf4j
public class SseEventListener implements ApplicationListener<SseEvent> {
    @Override
    public void onApplicationEvent(SseEvent sseEvent) {
        SseEvent.SseOperation operation = sseEvent.getSseOperation();
        try {
            if (operation == null || operation.getEmitter() == null) {
                log.warn("SseEvent is null or emitter is null");
                return;
            }
            SseEmitter emitter = operation.getEmitter();
            String message = getMessage(operation);
            if (message != null) {
                emitter.send(message, MediaType.TEXT_PLAIN);
            }
            if (operation.getCompleted() != null && operation.getCompleted()) {
                if (operation.getException() == null) {
                    emitter.complete();
                } else {
                    emitter.completeWithError(operation.getException());
                }
            }
        } catch (Exception e) {
            log.warn("SseEventListener error, operation: {}", operation, e);
        }
    }

    private static String getMessage(SseEvent.SseOperation operation) {
        if (operation.getQuiet() == null || !operation.getQuiet()) {
            JSONObject message = new JSONObject(new LinkedHashMap<>());
            if (operation.getEvent() != null) {
                message.put("event", operation.getEvent());
            }
            if (operation.getExecutionId() != null) {
                message.put("execution_id", operation.getExecutionId());
            }
            if (operation.getAnswerTaskName() != null) {
                message.put("answer_task_name", operation.getAnswerTaskName());
            }
            if (operation.getTaskName() != null) {
                message.put("task_name", operation.getTaskName());
            }
            if (operation.getException() != null) {
                message.put("exception", operation.getException().getMessage());
            }
            if (operation.getMessage() != null) {
                message.put("message", operation.getMessage());
            }
            message.put("type", operation.getType());
            message.put("source", "rill-flow sse-executor");
            return message.toJSONString();
        }
        if ("message".equals(operation.getEvent())) {
            return operation.getMessage();
        }
        return null;
    }
}
