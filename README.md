# Rill Flow

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html) [![EN doc](https://img.shields.io/badge/document-中文-red.svg)](README_zh_CN.md)

## Overview

Rill Flow is a high-performance and scalable distributed workflow orchestration service, originally designed to tackle the complexities and high concurrency demands of video processing workflows in the Weibo video business context. Key features of Rill Flow include user-friendliness, high concurrency, and low latency, which are essential for efficient task management. Currently, Rill Flow is extensively utilized in Weibo's video publishing and transcoding operations, handling tens of millions of tasks daily and supporting core processes of the video services.

Additionally, with the widespread adoption of Large Language Models (LLMs), Rill Flow has undergone optimizations based on cloud-native architecture and adapted for LLMs through plugin integration. This enhancement significantly improves scalability, enabling swift support for AI-generated content (AIGC) workflow integration.

## QuickStart

### Tool Preparation

Before you begin, ensure that the following tools are installed:

- Environment suitable for OSX/Linux
- [Docker](https://docs.docker.com/engine/install/)
- [Docker-Compose](https://docs.docker.com/compose/install/)

### Service Deployment

Install Rill Flow services on your local environment using Docker-Compose:

```shell
cat << EOF > docker-compose.yaml
version: '3'
services:
  rill-flow:
    image: weibocom/rill-flow
    depends_on:
      - cache
      - jaeger
    ports:
      - "8080:8080"
    environment:
      - RILL_FLOW_DESCRIPTOR_REDIS_HOST=cache
      - RILL_FLOW_DEFAULT_REDIS_HOST=cache
      - RILL_FLOW_TRACE_ENDPOINT=http://jaeger:4317
      - RILL_FLOW_CALLBACK_URL=http://rill-flow:8080/flow/finish.json
  cache:
    image: redis:6.2-alpine
    restart: always
    command: redis-server --save 20 1 --loglevel warning
  jaeger:
    image: jaegertracing/all-in-one:1.39
    restart: always
    environment:
      - COLLECTOR_OTLP_ENABLED=true
  ui:
    image: weibocom/rill-flow-ui
    ports:
      - "8088:80"
      - "8089:8089"
    depends_on:
      - rill-flow
      - jaeger
    environment:
      - BACKEND_SERVER=http://rill-flow:8080
      - TRACE_SERVER=http://jaeger:16686
EOF
docker-compose up -d
```

Once deployed, you can access the Rill Flow management console at <http://localhost:8088>.

### Execution Example

- Step 1: Submit a YAML file defining a workflow

```curl
curl --location  --request POST 'http://127.0.0.1:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSimple&feature_name=greet&alias=release' \
--header 'Content-Type: text/plain' \
--data-raw '---
version: 1.0.0
workspace: rillFlowSimple
dagName: greet
type: flow
tasks:
  - category: function
    name: Bob 
    resourceName: http://127.0.0.1:8080/flow/sample/greet.json?user=Bob
    pattern: task_sync
    tolerance: false
    next: Alice
    inputMappings:
      - source: "$.context.Bob"
        target: "$.input.Bob"
  - category: function
    name: Alice 
    resourceName: http://127.0.0.1:8080/flow/sample/greet.json?user=Alice
    pattern: task_sync
    tolerance: false
    inputMappings:
      - source: "$.context.Alice"
        target: "$.input.Alice"
'
```

- Step 2: Submit a task to execute the workflow
  
```curl
curl -XPOST 'http://127.0.0.1:8080/flow/submit.json?descriptor_id=rillFlowSimple:greet'  -d '{"Bob":"Hello, I am Bob!", "Alice": "Hi, I am Alice"}' -H 'Content-Type:application/json'
```

- Step 3：Query the task execution results

  Query the execution details via the Rill Flow management console.(admin/admin)

```curl
http://127.0.0.1:8080/#/flow-instance/list
```

## Document
https://rill-flow.github.io/en/docs/intro

## Contributors

The following are contributors to the project along with their GitHub links:

- axb       ([@qdaxb](https://github.com/qdaxb))
- Ocean     ([@hhh041](https://github.com/hhh041))
- xilong-t  ([@xilong-t](https://github.com/xilong-t))
- techlog   ([@techloghub](https://github.com/techloghub))
- ch15084   ([@ch15084](https://github.com/ch15084))
- qfl       ([@qiaofenlin](https://github.com/qiaofenlin))
- Kylen     ([@Kylen](https://github.com/Kylen0714))
- zzfzzf    ([@zzfzzf](https://github.com/zzfzzf))
- feifei    ([@feifei325](https://github.com/feifei325))

## License

Rill Flow is an open-source project under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
