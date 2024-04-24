# Rill Flow

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html) ![codecov](https://codecov.io/gh/weibocom/rill-flow/branch/main/graph/badge.svg) [![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)

## 概述

Rill Flow是一款高性能、可扩展的分布式流程编排服务，具备以下核心特性:

- 高性能: 日支持千万级任务执行,执行延迟低于100ms
- 分布式: 支持异构分布式系统的编排调度
- 易用性: 支持可视化流程编排、插件化接入
- 云原生: 支持云原生容器化部署及云原生函数编排
- AIGC: 支持对LLM模型服务的快速集成

## 演示
[在线体验地址](https://rill-flow.cloud) (sandbox/sandbox)

## 快速开始

### 工具准备

在开始之前，请确保安装了以下所需工具：

- 适用于OSX/Linux的环境
- [Docker](https://docs.docker.com/engine/install/)
- [Docker-Compose](https://docs.docker.com/compose/install/)

### 服务部署

#### 下载Rill-Flow源码
```shell
git clone https://github.com/weibocom/rill-flow.git
```

#### 启动服务
进入rill-flow源代码的docker目录，执行一键启动命令:

```shell
cd rill-flow/docker
docker-compose up -d
```
> 如果您的系统安装了 Docker Compose V2 而不是 V1，请使用 `docker compose` 而不是 `docker-compose`。通过`docker compose version`检查这是否为情况。在[这里](https://docs.docker.com/compose/#compose-v2-and-the-new-docker-compose-command)阅读更多信息。

### 验证安装

要查看 Rill Flow 的运行情况，请执行以下命令：

```shell
docker-compose ps
```

以下是预期输出：

```txt
           Name                         Command               State                                           Ports
------------------------------------------------------------------------------------------------------------------------------------------------------------
rill-flow-mysql              docker-entrypoint.sh --bin ...   Up      0.0.0.0:3306->3306/tcp, 33060/tcp
rillflow_cache_1             docker-entrypoint.sh redis ...   Up      6379/tcp
rillflow_jaeger_1            /go/bin/all-in-one-linux         Up      14250/tcp, 14268/tcp, 0.0.0.0:16686->16686/tcp, 5775/udp, 5778/tcp, 6831/udp, 6832/udp
rillflow_rill-flow_1         catalina.sh run                  Up      0.0.0.0:8080->8080/tcp
rillflow_sample-executor_1   uvicorn main:app --host 0. ...   Up
rillflow_ui_1                /docker-entrypoint.sh /bin ...   Up      0.0.0.0:80->80/tcp
```

如果你的实际输出与预期输出相符，表示 Rill Flow 已经成功安装。

### 访问Rill Flow 管理后台

执行成功后，可通过 `http://localhost` (admin/admin)访问 Rill Flow 管理后台。若为服务端部署，则直接使用服务器IP进行访问(端口默认为80)。

### 提交任务

#### 提交简单流程任务
- Step 1: 打开 Rill Flow 管理后台，点击 `流程定义` 菜单，进入`流程列表`页面, 点击`新建`按钮。
- Step 2: 进入`新建流程`页面后，打开`一键导入`开关，将以下yaml文件内容复制到文本框中，点击`提交`按钮，即可提交简单的流程图。
```yaml
version: 1.0.0
workspace: rillFlowSimple
dagName: greet
alias: release
type: flow
inputSchema: >-
  [{"required":true,"name":"Bob","type":"String"},{"required":true,"name":"Alice","type":"String"}]
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
```


- Step 3: 提交流程图执行任务

点击`测试`按钮，填写所需参数后，点击`提交`按钮。

- Step 4: 查看执行结果
  上一步点击`提交`按钮后会自动跳转到执行详情页。可通过点击`执行详情`按钮查看执行状态和详细内容。

> 更多关于查看结果的说明可以参考[执行状态](../user-guide/04-execution/03-status.md)

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
