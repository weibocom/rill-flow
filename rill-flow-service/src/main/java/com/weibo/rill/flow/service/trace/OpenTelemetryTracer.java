package com.weibo.rill.flow.service.trace;

import io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace.Span;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class OpenTelemetryTracer implements Tracer {
    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";

    @Autowired
    private Environment environment;

    public String traceId() {
        Span currentSpan = Span.current();
        if (Objects.isNull(currentSpan)) {
            return null;
        }
        String traceId = currentSpan.getSpanContext().getTraceId();
        if (StringUtils.isEmpty(traceId) || StringUtils.equals(traceId, INVALID_TRACE_ID)) {
            return null;
        }
        return traceId;

    }

    public  boolean isOpenTrace(){
        String environmentValue = environment.getProperty("RILL_FLOW_TRACE_ENDPOINT");
        if (StringUtils.isBlank(environmentValue)) {
            return false;
        }
        return true;
    }
}
