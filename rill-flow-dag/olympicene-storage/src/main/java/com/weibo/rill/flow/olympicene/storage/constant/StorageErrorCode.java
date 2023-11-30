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

package com.weibo.rill.flow.olympicene.storage.constant;

public enum StorageErrorCode {

    NONSUPPORT(1, "nonsupport"),
    LOCK_TIMEOUT(2, "lock_timeout"),
    SERIALIZATION_FAIL(3, "serialization fail"),
    RESOURCE_NOT_FOUND(4, "resource not found"),
    CLASS_TYPE_NONSUPPORT(5, "class type nonsupport"),
    CONTEXT_GET_FAIL(6, "context get fail"),
    CONTEXT_LENGTH_LIMITATION(7, "context length limitation"),
    DAG_LENGTH_LIMITATION(8, "dag length limitation")
    ;

    private static final int BASE_ERROR_CODE = 30500;
    private final int code;
    private final String message;

    StorageErrorCode(final int code, final String message) {
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
