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

package com.weibo.rill.flow.common.model;

import com.alibaba.fastjson.JSONObject;

public enum BizError {
    ERROR_INTERNAL(1, "internal server error"),
    ERROR_URI(2, "incorrect uri"),
    ERROR_AUTH(4, "auth failed"),
    ERROR_UNSUPPORTED(5, "unsupported"),
    ERROR_PROCESS_FAIL(6, "process fail"),
    ERROR_DEGRADED(7, "feature degraded"),
    ERROR_FORBIDDEN(8, "forbidden"),
    /**
     * 数据格式错误
     */
    ERROR_DATA_FORMAT(9, "error data format"),
    /**
     * 数据不合法
     */
    ERROR_DATA_RESTRICTION(10, "restriction violation"),
    ERROR_HYSTRIX(11, "hystrix"),
    ERROR_MISSING_PARAMETER(12, "missing parameter args"),
    ERROR_INVOKE_URI(13, "error invoke uri"),

    ERROR_REDIRECT_URL(14, "error redirect url"),

    ERROR_RUNTIME_STORAGE_USAGE_LIMIT(100, "dag runtime storage usage limit"),
    ERROR_RUNTIME_RESOURCE_STATUS_LIMIT(101, "dag runtime resource status limit"),
    ;

    private static final int BASE_ERROR_CODE = 30100;

    private static final BizError[] errors = BizError.values();

    private final int code;
    private final String causeMsg;

    BizError(final int code, final String causeMsg) {
        this.code = code;
        this.causeMsg = causeMsg;
    }

    public static BizError valueOf(int code) {
        int realCode = code - BASE_ERROR_CODE;
        for (BizError error : errors) {
            if (error.code == realCode) {
                return error;
            }
        }
        return BizError.ERROR_INTERNAL;
    }

    public static JSONObject toJson(BizError bizError) {
        JSONObject errorInfo = new JSONObject();
        errorInfo.put("error", bizError.causeMsg);
        errorInfo.put("error_code", bizError.getCode());
        return errorInfo;
    }

    public int getCode() {
        return BASE_ERROR_CODE + code;
    }

    public String getCauseMsg() {
        return causeMsg;
    }

}
