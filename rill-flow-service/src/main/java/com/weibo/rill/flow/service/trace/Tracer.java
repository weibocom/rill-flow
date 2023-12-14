package com.weibo.rill.flow.service.trace;

public interface Tracer {
    String traceId();

    boolean isOpenTrace();
}
