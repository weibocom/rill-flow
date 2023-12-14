package com.weibo.rill.flow.service.trace;

import java.util.Map;
import java.util.Optional;

public class TraceableContextWrapper {
    private Map<String,Object> context;
    public TraceableContextWrapper(Map<String, Object> context) {
        this.context = context;
    }

    public void setTraceId(String traceId) {
        context.put("traceId", traceId);
    }

    public String getTraceId() {
        return Optional.ofNullable(context.get("traceId")).map(Object::toString).orElse(null);
    }
}
