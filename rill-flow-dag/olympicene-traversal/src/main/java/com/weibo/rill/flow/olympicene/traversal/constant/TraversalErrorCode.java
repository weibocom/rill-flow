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

package com.weibo.rill.flow.olympicene.traversal.constant;

public enum TraversalErrorCode {

    DAG_ILLEGAL_STATE(1, "dag illegal state."),
    DAG_NOT_FOUND(2, "dag not found."),
    TRAVERSAL_FAILED(3, "traversal failed."),
    DAG_ALREADY_EXIST(4, "execution %s is already running."),
    DAG_EXECUTION_NOT_FOUND(5, "execution %s not found."),
    OPERATION_UNSUPPORTED(6, "operation unsupported")
    ;

    private static final int BASE_ERROR_CODE = 30600;
    private final int code;
    private final String message;

    TraversalErrorCode(final int code, final String causeMsg) {
        this.code = code;
        this.message = causeMsg;
    }

    public int getCode() {
        return BASE_ERROR_CODE + code;
    }

    public String getMessage() {
        return message;
    }

}
