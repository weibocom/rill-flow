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

package com.weibo.rill.flow.olympicene.traversal.service;

import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace.Span;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
public class TraceService {
    private static final Logger logger = LoggerFactory.getLogger(TraceService.class);

    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";

    @Autowired
    private DAGContextStorage dagContextStorage;

    @Autowired
    private Environment environment;

    public void setTraceId(Map<String, Object> data) {
        if (!isOpenTrace()) {
            return;
        }
        Span currentSpan = Span.current();
        if (Objects.isNull(currentSpan)) {
            return;
        }
        String traceId = currentSpan.getSpanContext().getTraceId();
        if (StringUtils.isEmpty(traceId) || StringUtils.equals(traceId, INVALID_TRACE_ID)) {
            return;
        }
        if (data == null) {
            data = Maps.newHashMap();
            data.put("traceId", traceId);
        } else {
            data.put("traceId", traceId);
        }
    }

    public String getTraceId(String executionId) {
        Map<String, Object> context = dagContextStorage.getContext(executionId);
        if (MapUtils.isEmpty(context)) {
            return StringUtils.EMPTY;
        }
        return (String) context.getOrDefault("traceId", StringUtils.EMPTY);
    }

    public boolean isOpenTrace() {
        String environmentValue = environment.getProperty("RILL_FLOW_TRACE_ENDPOINT");
        logger.info("env RILL_FLOW_TRACE_ENDPOINT value:{}", environmentValue);
        if (StringUtils.isBlank(environmentValue)) {
            return false;
        }
        return true;
    }
}
