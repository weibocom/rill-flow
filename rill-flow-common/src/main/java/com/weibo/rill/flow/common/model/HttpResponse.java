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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class HttpResponse {

    @JsonProperty("request_id")
    private String requestId;

    @JsonProperty("error_code")
    private Integer errorCode;

    @JsonProperty("error")
    private String errorMsg;

    @JsonProperty("pass_through")
    private Boolean passThrough;

    @JsonProperty("data")
    private Object data;

    private HttpResponse() {
    }

    public static HttpResponse error(final int errorCode, final String errorMsg) {
        return buildHttpResponse(errorCode, errorMsg);
    }

    public static HttpResponse data(final Object object) {
        HttpResponse resp = new HttpResponse();
        resp.data = object;
        return resp;
    }

    public static HttpResponse error(BizError bizError) {
        return buildHttpResponse(bizError.getCode(), bizError.getCauseMsg());
    }

    public static HttpResponse error(final int errorCode, final String errorMsg, final Boolean passThrough) {
        HttpResponse resp = buildHttpResponse(errorCode, errorMsg);
        resp.passThrough = passThrough;
        return resp;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    private static HttpResponse buildHttpResponse(final int errorCode, final String errorMsg) {
        HttpResponse resp = new HttpResponse();
        resp.errorCode = errorCode;
        resp.errorMsg = errorMsg;
        return resp;
    }

    @Override
    public String toString() {
        return "ErrorResponse{" + "errorCode=" + errorCode + ", create='" + errorMsg + '\'' + ", request='" + requestId + '\'' + '}';
    }

}
