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

package com.weibo.rill.flow.olympicene.core.constant;

public enum CoreErrorCode {
    TASK_NAME_DUPLICATED(1, "task name duplicated"),
    TASK_ILLEGAL_STATE(2, "task illegal state"),
    DAG_STATE_NONSUPPORT(3, "dag state nonsupport")
    ;

    private static final int BASE_ERROR_CODE = 30300;
    private final int code;
    private final String message;

    CoreErrorCode(final int code, final String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return BASE_ERROR_CODE + code;
    }

    public String getMessage() {
        return message;
    }
}
