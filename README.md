# Rill Flow

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html) ![codecov](https://codecov.io/gh/weibocom/rill-flow/branch/main/graph/badge.svg) [![CN doc](https://img.shields.io/badge/document-中文-blue.svg)](README_zh_CN.md)

## Overview

Rill Flow is a high-performance, scalable distributed workflow orchestration service with the following core features:

- High performance: Supports the execution of tens of millions of tasks per day, with task execution latency less than 100ms
- Distributed: Supports the orchestration and scheduling of heterogeneous distributed systems
- Ease to use: supports visual process orchestration and plug-in access
- Cloud native: Supports cloud native container deployment and cloud native function orchestration
- AIGC: supports rapid integration of LLM model services

## Demo
[Live Demo](https://rill-flow.cloud) (sandbox/sandbox)

## QuickStart

### Environment Preparation

Before you begin, ensure that the following tools are installed:

- Environment suitable for OSX/Linux
- [Docker](https://docs.docker.com/engine/install/)
- [Docker-Compose](https://docs.docker.com/compose/install/)

### Service Deployment
Install Rill Flow services on your local environment using Docker-Compose:

#### Download the Rill-Flow source code.
```shell
git clone https://github.com/weibocom/rill-flow.git
```

#### Start the service.
Enter the Docker directory of the Rill-Flow source code and execute the one-click start command:

```shell
cd rill-flow/docker
docker-compose up -d
```
> If your system has Docker Compose V2 installed instead of V1, please use docker compose instead of docker-compose. Check if this is the case by running docker compose version. Read more information [here](https://docs.docker.com/compose/#compose-v2-and-the-new-docker-compose-command).

### Verify the installation.

To check the status of Rill Flow, please execute the following command:

```shell
docker-compose ps
```

Here is the expected output:

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

If your actual output matches the expected output, it means that Rill Flow has been successfully installed.

### Access the Rill Flow administration background

After the command is successfully executed, you can access the Rill Flow management background at http://localhost (admin/admin). If the server is deployed, use the server IP address for access (port 80 by default).

### Execution Example

- Step 1: Open the Rill Flow management background, click the 'Flow Definition' menu, enter the 'Flow Definition List' page, click the 'Create' button
- Step 2: After entering the 'Flow Graph Edit' page, open the 'one-click import' switch, copy the following yaml file content into the text box, click the 'Submit' button, you can submit a simple flowchart.
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


- Step 3: Submit the flow graph to execute the task

Click the 'Test' button, fill in the required parameters, and click the 'Submit' button.

- Step 4: Viewing the execution Result
  Click the 'Submit' button in the previous step and you will automatically jump to the execution details page. You can view the execution status and details by clicking the 'Execution Records' button.

> More instructions on viewing results can be found in [Execution Status](../user-guide/04-execution/03-status.md)

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
