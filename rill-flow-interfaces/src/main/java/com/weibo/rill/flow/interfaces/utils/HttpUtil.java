package com.weibo.rill.flow.interfaces.utils;

import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpUtil {

    public static HttpParameter functionRequestParams(Resource resource, DispatchInfo dispatchInfo) {
        String executionId = dispatchInfo.getExecutionId();
        Map<String, Object> input = dispatchInfo.getInput();
        String taskInfoName = dispatchInfo.getTaskInfo().getTask().getName();

        return functionRequestParams(executionId, taskInfoName, resource, input);
    }

    public static HttpParameter functionRequestParams(String executionId, String taskInfoName, Resource resource, Map<String, Object> input) {
        HttpParameter httpParameter = buildRequestParams(executionId, input);
        Map<String, Object> queryParams = httpParameter.getQueryParams();
        queryParams.put("name", taskInfoName);
        Map<String, Object> body = httpParameter.getBody();
        httpParameter.setBody(Optional.ofNullable((Map<String, Object>) body.get("data")).orElse(new HashMap<>()));
        return httpParameter;
    }

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
                queryParams.putAll((Map) value);
            } else if (key.startsWith("request_config_") && value instanceof Map) {
                body.putAll((Map) value);
            } else if (key.startsWith("request_callback_") && value instanceof Map) {
                callback.putAll((Map) value);
            } else {
                functionInput.put(key, value);
            }
        }));

        return HttpParameter.builder().queryParams(queryParams).body(body).callback(callback).build();
    }
}
