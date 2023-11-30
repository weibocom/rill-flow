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

package com.weibo.rill.flow.interfaces.model.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum FunctionPattern {
    TASK_ASYNC("task_async"),          // 任务级别调用 异步
    TASK_SCHEDULER("task_scheduler"),  // 任务级别调用 异步
    TASK_SYNC("task_sync"),            // 任务级别调用 同步
    FLOW_ASYNC("flow_async"),          // 流程级别调用 异步
    FLOW_SYNC("flow_sync"),            // 流程级别调用 同步
    ;

    private final String value;

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static FunctionPattern forValues(String value) {
        for (FunctionPattern item : FunctionPattern.values()) {
            if (item.value.equals(value)) {
                return item;
            }
        }

        return null;
    }
}
