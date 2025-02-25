package com.weibo.rill.flow.olympicene.traversal.helper;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class TracerHelper {
    private RedisClient redisClient;
    @Getter
    private Tracer tracer;

    public TracerHelper(RedisClient redisClient, Tracer tracer) {
        this.redisClient = redisClient;
        this.tracer = tracer;
    }

    // Redis key 前缀
    private static final String TRACE_KEY_PREFIX = "rill_flow_trace_";
    // 设置合适的过期时间（例如24小时）
    private static final int TRACE_EXPIRE_SECONDS = 2 * 60 * 60;
    private static final String EXECUTION_TRACE_KEY_PREFIX = "rill_flow_execution_trace_";

    public void removeSpanContext(String executionId, String taskId) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            redisClient.del(key.getBytes());
        } catch (Exception e) {
            log.error("Failed to remove span context from Redis for task: {}", taskId, e);
        }
    }

    public void saveSpan(String executionId, String taskId, Context parentContext, Span currentSpan) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            JSONObject contextInfo = new JSONObject();
            SpanContext spanContext = currentSpan.getSpanContext();
            SpanContext parentSpanContext = Span.fromContext(parentContext).getSpanContext();
            
            contextInfo.put("traceId", spanContext.getTraceId());
            contextInfo.put("spanId", spanContext.getSpanId());
            contextInfo.put("parentSpanId", parentSpanContext.getSpanId());
            contextInfo.put("traceFlags", spanContext.getTraceFlags().asHex());
            contextInfo.put("startTime", System.currentTimeMillis());  // 保存开始时间

            redisClient.set(key, contextInfo.toJSONString());
            redisClient.expire(key, TRACE_EXPIRE_SECONDS);
        } catch (Exception e) {
            log.error("Failed to save context to Redis for task: {}", taskId, e);
        }
    }

    public Span loadSpan(String executionId, String taskId) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            String contextInfoString = redisClient.get(key);

            if (contextInfoString == null || contextInfoString.isEmpty()) {
                return null;
            }

            JSONObject contextInfo = JSONObject.parseObject(contextInfoString);
            String traceId = contextInfo.getString("traceId");
            String spanId = contextInfo.getString("spanId");
            String parentSpanId = contextInfo.getString("parentSpanId");
            String traceFlags = contextInfo.getString("traceFlags");
            long startTime = Long.parseLong(contextInfo.getString("startTime"));

            SpanContext parentContext = SpanContext.create(
                    traceId,
                    parentSpanId,
                    TraceFlags.fromHex(traceFlags, 0),
                    TraceState.getDefault()
            );

            return tracer.spanBuilder("runTask " + taskId)
                    .setParent(Context.current().with(Span.wrap(parentContext)))
                    .setAttribute("original.span.id", spanId)
                    .setStartTimestamp(startTime, java.util.concurrent.TimeUnit.MILLISECONDS)  // 设置正确的开始时间
                    .startSpan();
        } catch (Exception e) {
            log.error("Failed to load span from Redis for task: {}", taskId, e);
            return null;
        } finally {
            removeSpanContext(executionId, taskId);
        }
    }

    public void saveExecutionContext(String executionId, Context context) {
        try {
            String key = EXECUTION_TRACE_KEY_PREFIX + executionId;
            Span span = Span.fromContext(context);
            SpanContext spanContext = span.getSpanContext();
            JSONObject contextInfo = new JSONObject();
            contextInfo.put("traceId", spanContext.getTraceId());
            contextInfo.put("spanId", spanContext.getSpanId());
            contextInfo.put("traceFlags", spanContext.getTraceFlags().asHex());
            contextInfo.put("traceState", spanContext.getTraceState().toString());
            
            redisClient.set(key, contextInfo.toJSONString());
            redisClient.expire(key, TRACE_EXPIRE_SECONDS);
        } catch (Exception e) {
            log.error("Failed to save execution context to Redis for execution: {}", executionId, e);
        }
    }

    public Context loadExecutionContext(String executionId) {
        try {
            String key = EXECUTION_TRACE_KEY_PREFIX + executionId;
            String contextInfoString = redisClient.get(key);

            if (contextInfoString == null || contextInfoString.isEmpty()) {
                return null;
            }

            JSONObject contextInfo = JSONObject.parseObject(contextInfoString);
            String traceId = contextInfo.getString("traceId");
            String spanId = contextInfo.getString("spanId");
            String traceFlags = contextInfo.getString("traceFlags");
            // We'll use default TraceState since parsing from string isn't available
            TraceState traceState = TraceState.getDefault();

            SpanContext spanContext = SpanContext.create(
                    traceId,
                    spanId,
                    TraceFlags.fromHex(traceFlags, 0),
                    traceState
            );

            return Context.current().with(Span.wrap(spanContext));
        } catch (Exception e) {
            log.error("Failed to load execution context from Redis for execution: {}", executionId, e);
            return null;
        }
    }
}
