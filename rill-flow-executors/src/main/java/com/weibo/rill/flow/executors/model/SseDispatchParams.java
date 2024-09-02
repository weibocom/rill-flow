package com.weibo.rill.flow.executors.model;

import lombok.Data;

import java.util.Map;

@Data
public class SseDispatchParams {
    String url;
    Map<String, Object> input;
    String requestType;
    Map<String, Object> headers;
}
