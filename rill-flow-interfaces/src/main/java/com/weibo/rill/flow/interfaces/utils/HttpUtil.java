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

package com.weibo.rill.flow.interfaces.utils;

import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpUtil {

    public static HttpParameter functionRequestParams(DispatchInfo dispatchInfo) {
        String executionId = dispatchInfo.getExecutionId();
        Map<String, Object> input = dispatchInfo.getInput();
        String taskInfoName = dispatchInfo.getTaskInfo().getTask().getName();

        return functionRequestParams(executionId, taskInfoName, input);
    }

    @SuppressWarnings("unchecked")
    public static HttpParameter functionRequestParams(String executionId, String taskInfoName, Map<String, Object> input) {
        HttpParameter httpParameter = buildRequestParams(executionId, input);
        Map<String, Object> queryParams = httpParameter.getQueryParams();
        queryParams.put("name", taskInfoName);
        Map<String, Object> body = httpParameter.getBody();
        httpParameter.setBody(Optional.ofNullable((Map<String, Object>) body.get("data")).orElse(new HashMap<>()));
        return httpParameter;
    }

    @SuppressWarnings("unchecked")
    public static HttpParameter buildRequestParams(String executionId, Map<String, Object> input) {
        Map<String, Object> queryParams = new HashMap<>();
        Map<String, Object> body = new HashMap<>();
        Map<String, Object> functionInput = new HashMap<>();
        Map<String, Object> callback = new HashMap<>();

        queryParams.put("execution_id", executionId);
        body.put("data", functionInput);
        body.put("group_id", executionId);
        Optional.ofNullable(input).ifPresent(inputMap -> inputMap.forEach((key, value) -> {
            if (key.startsWith("query_params_") && value instanceof Map) {
                queryParams.putAll((Map<String, Object>) value);
            } else if (key.startsWith("request_config_") && value instanceof Map) {
                body.putAll((Map<String, Object>) value);
            } else if (key.startsWith("request_callback_") && value instanceof Map) {
                callback.putAll((Map<String, Object>) value);
            } else {
                functionInput.put(key, value);
            }
        }));

        return HttpParameter.builder().queryParams(queryParams).body(body).callback(callback).build();
    }
}
