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

package com.weibo.rill.flow.impl.constant;

public enum FlowErrorCode {

    AUTH_FAILED(1, "auth failed."),

    AUTH_EXPIRED(2, "auth expired."),

    NO_SIGN(3, "no SIGN.")
    ;
    private final int code;
    private final String message;

    FlowErrorCode(final int code, final String causeMsg) {
        this.code = code;
        this.message = causeMsg;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    }
