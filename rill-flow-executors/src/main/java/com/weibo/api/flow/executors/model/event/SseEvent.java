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

package com.weibo.api.flow.executors.model.event;

import com.weibo.api.flow.executors.model.enums.AnswerExpressionMetaType;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Getter
public class SseEvent extends ApplicationEvent {
    private final SseOperation sseOperation;

    public SseEvent(SseOperation source) {
        super(source);
        this.sseOperation = source;
    }

    @Data
    @Builder
    public static class SseOperation {
        private String event;
        private String executionId;
        private String answerTaskName;
        private String taskName;
        private String message;
        private Exception exception;
        private SseEmitter emitter;
        private Boolean completed;
        private Boolean quiet;
        private AnswerExpressionMetaType type;
    }
}
