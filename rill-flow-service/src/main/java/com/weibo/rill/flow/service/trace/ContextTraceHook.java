package com.weibo.rill.flow.service.trace;

import com.weibo.rill.flow.service.context.ContextInitializeHook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Component
public class ContextTraceHook implements ContextInitializeHook<Map<String, Object>> {

    @Autowired
    private Tracer tracer;

    @Override
    public Map<String, Object> initialize(Map<String, Object> context) {
        if (tracer.isOpenTrace()) {
            Optional.ofNullable(tracer.traceId()).ifPresent((traceId) -> new TraceableContextWrapper(context).setTraceId(traceId));
        }
        return context;
    }

}
