#!/bin/sh

if [ ! -z "$RILL_FLOW_TRACE_ENDPOINT" ]; then
  export JAVA_TOOL_OPTIONS=-javaagent:/usr/local/tomcat/temp/opentelemetry-javaagent.jar
  export OTEL_EXPORTER_OTLP_ENDPOINT=$RILL_FLOW_TRACE_ENDPOINT
  export OTEL_SERVICE_NAME=rill-flow
  export OTEL_INSTRUMENTATION_MICROMETER_ENABLED=false
  export OTEL_METRICS_EXPORTER=none
  export OTEL_EXPORTER=jaeger
  export OTEL_INSTRUMENTATION_JEDIS_ENABLED=false
  export JAVA_OPTS="$JAVA_OPTS -Dotel.java.global-autoconfigure.enabled=true"
fi