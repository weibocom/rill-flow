# Rill Flow

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html) ![codecov](https://codecov.io/gh/weibocom/rill-flow/branch/main/graph/badge.svg) [![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)

## 概述

Rill Flow是一款高性能、可扩展的分布式流程编排服务，具备以下核心特性:

- 高性能: 日支持千万级任务执行,执行延迟低于100ms
- 分布式: 支持异构分布式系统的编排调度
- 易用性: 支持可视化流程编排、插件化接入
- 云原生: 支持云原生容器化部署及云原生函数编排
- AIGC: 支持对LLM模型服务的快速集成

## 快速开始

### 工具准备

在开始之前，请确保安装了以下所需工具：

- 适用于OSX/Linux的环境
- [Docker](https://docs.docker.com/engine/install/)
- [Docker-Compose](https://docs.docker.com/compose/install/)

## 服务部署

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

## 验证安装

要查看 Rill Flow 的运行情况，请执行以下命令：

```shell
docker-compose ps
```

以下是预期输出：

```txt
     Name                    Command               State                                    Ports
----------------------------------------------------------------------------------------------------------------------------------
tmp_cache_1       docker-entrypoint.sh redis ...   Up      6379/tcp
tmp_jaeger_1      /go/bin/all-in-one-linux         Up      14250/tcp, 14268/tcp, 16686/tcp, 5775/udp, 5778/tcp, 6831/udp, 6832/udp
tmp_rill-flow_1   catalina.sh run                  Up      0.0.0.0:8080->8080/tcp
tmp_ui_1          /docker-entrypoint.sh /bin ...   Up      0.0.0.0:8088->80/tcp, 0.0.0.0:8089->8089/tcp
```

如果你的实际输出与预期输出相符，表示 Rill Flow 已经成功安装。

## 访问Rill Flow 管理后台

执行成功后，可通过 `http://localhost:8088` (admin/admin)访问 Rill Flow 管理后台。

## 提交任务

### 提交简单流程任务

- Step 1: 提交 YAML 文件定义的流程图

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

- Step 2: 提交流程图执行任务

```curl
curl -X POST 'http://127.0.0.1:8080/flow/submit.json?descriptor_id=rillFlowSimple:greet'  -d '{"Bob":"Hello, I am Bob!", "Alice": "Hi, I am Alice"}' -H 'Content-Type:application/json'
```

### 查看运行结果

- 打开Rill Flow管理后台查询执行详情

```cURL
http://127.0.0.1:8088/#/flow-instance/list
```

![preview](https://rill-flow.github.io/img/flow_sample.jpg)

## 文档

- [中文文档](https://rill-flow.github.io/docs/intro)

## 贡献者

以下是项目贡献者及其 GitHub 链接：

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

## 开源协议

Rill Flow 是在 [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) 协议下的开源项目。
