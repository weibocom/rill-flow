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

package com.weibo.rill.flow.olympicene.core.model.dag;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum DAGStatus {

    // 初始状态
    NOT_STARTED("not_started"),

    // 执行中
    RUNNING("running"),

    // 关键路径完成
    KEY_SUCCEED("key_succeed"),

    // 成功
    SUCCEED("succeed"),

    // 失败
    FAILED("failed"),
    ;

    private final String value;

    @JsonCreator
    public static DAGStatus parse(String value) {
        for(DAGStatus status : values()) {
            if (status.getValue().equals(value)) {
                return status;
            }
        }
        return null;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    /**
     * 无论成功与否的完成
     */
    public boolean isCompleted() {
        return this.ordinal() >= SUCCEED.ordinal();
    }

    public boolean isSuccess() {
        return this == SUCCEED;
    }
}
