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

package com.weibo.rill.flow.olympicene.ddl.constant;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum DDLErrorCode {
    DAG_DESCRIPTOR_EMPTY(1, "dag descriptor empty."),
    DAG_DESCRIPTOR_INVALID(2, "dag descriptor invalid."),
    DAG_TYPE_INVALID(3, "dag type is invalid."),
    SERIALIZATION_FAIL(4, "serialization failed."),
    DAG_TASK_EMPTY(5, "dag task empty."),
    TASK_INVALID(6, "task %s is invalid. Because %s."),
    NAME_DUPLICATED(7, "name duplicated"),
    NAME_INVALID(8, "name invalid"),
    NOT_SUPPORTED_TASK(9, "task is not supported."),
    FUNCTION_TASK_INVALID(10, "function task %s is invalid. Because %s."),
    FOREACH_TASK_INVALID(11, "foreach task %s is invalid. Because %s."),
    CHOICE_TASK_INVALID(12, "choice task %s is invalid. Because %s."),
    TASK_NEXT_INVALID(13, "next task %s not exist.")
    ;

    private static final int BASE_ERROR_CODE = 30400;
    private final int code;
    private final String message;

    public int getCode() {
        return BASE_ERROR_CODE + code;
    }

    public String getMessage() {
        return message;
    }
}
