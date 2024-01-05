# Rill Flow

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html) ![codecov](https://codecov.io/gh/weibocom/rill-flow/branch/main/graph/badge.svg) [![CN doc](https://img.shields.io/badge/document-中文-blue.svg)](README_zh_CN.md)

## Overview

Rill Flow is a high-performance, scalable distributed workflow orchestration service with the following core features:

- High performance: Supports the execution of tens of millions of tasks per day, with task execution latency less than 100ms
- Distributed: Supports the orchestration and scheduling of heterogeneous distributed systems
- Ease to use: supports visual process orchestration and plug-in access
- Cloud native: Supports cloud native container deployment and cloud native function orchestration
- AIGC: supports rapid integration of LLM model services

## QuickStart

### Environment Preparation

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
      - RILL_FLOW_TRACE_QUERY_HOST=http://jaeger:16686
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
    depends_on:
      - rill-flow
      - jaeger
    environment:
      - BACKEND_SERVER=http://rill-flow:8080
  sample-executor:
    image: weibocom/rill-flow-sample:sample-executor 
EOF
docker-compose up -d
```

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
    resourceName: http://sample-executor:8000/greet.json?user=Bob
    pattern: task_sync
    tolerance: false
    next: Alice
    inputMappings:
      - source: "$.context.Bob"
        target: "$.input.Bob"
  - category: function
    name: Alice 
    resourceName: http://sample-executor:8000/greet.json?user=Alice
    pattern: task_sync
    tolerance: false
    inputMappings:
      - source: "$.context.Alice"
        target: "$.input.Alice"
'
```

- Step 2: Submit a task to execute the workflow
  
```curl
curl -X POST 'http://127.0.0.1:8080/flow/submit.json?descriptor_id=rillFlowSimple:greet'  -d '{"Bob":"Hello, I am Bob!", "Alice": "Hi, I am Alice"}' -H 'Content-Type:application/json'
```

- Step 3：Query the task execution results

  Query the execution details via the Rill Flow UI.(admin/admin)

```curl
http://127.0.0.1:8088/#/flow-instance/list
```

![preview](https://rill-flow.github.io/img/flow_sample.jpg)

## Document

- [Document](https://rill-flow.github.io/en/docs/intro)
- [中文文档](https://rill-flow.github.io/docs/intro)

## Contributors

The following are contributors to the project along with their GitHub links:

- axb       ([@qdaxb](https://github.com/qdaxb)) *Maintainer*
- techlog   ([@techloghub](https://github.com/techloghub)) *Maintainer*
- ch15084   ([@ch15084](https://github.com/ch15084)) *Maintainer*
- Ocean     ([@hhh041](https://github.com/hhh041))
- xilong-t  ([@xilong-t](https://github.com/xilong-t))
- qfl       ([@qiaofenlin](https://github.com/qiaofenlin))
- Kylen     ([@Kylen](https://github.com/Kylen0714))
- zzfzzf    ([@zzfzzf](https://github.com/zzfzzf))
- feifei    ([@feifei325](https://github.com/feifei325))
- moqimoqidea    ([@moqimoqidea](https://github.com/moqimoqidea))
- Guo, Jiansheng ([@guojiansheng0925](https://github.com/guojiansheng0925))

## License

Rill Flow is an open-source project under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
