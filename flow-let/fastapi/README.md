# flow-let

Reverse proxy for HTTP microservices

The flow-let implements an HTTP server listening on port 8002(port can be customized).

The flow-let is a proxy component on the Rill Flow actuator. It runs on the node of the custom operator. The work of flow-let is a task agent and provides the task agency ability of synchronous renewal steps.

## Build
Users can use the provided Rill Flow packaging script to create a Docker image for the flow-let.
```shell
sh build_image.sh
```

## Deployment
When a custom executor is deployed in k8s, flow-let can be deployed together with the custom executor as a sidecar container.

[Deploy yaml](flow-let-sidecar.yaml) by modifying flow-let to achieve rapid deployment with flow-let.

Alternatively, modify the docker-compose file:
```shell
cat <<EOF > docker-compose-flow-let-sidecar.yaml
version: '3'
services:
  flow-let:
    image: weibocom/rill-flow-let:fastapi
    environment:
     upstream_url: http://{executor-host}:{port} 
EOF
docker-compose up -d
```

## Invoke

Invoke the executor using the Rill Flow [HTTP dispatcher](https://rill-flow.github.io/docs/user-guide/defination/task-and-dispatcher#http-协议派发器).

## Configuration

Environmental variables:

| Option  | Usage |
|---------| --- |
|  `upstream_url` | where to forward requests i.e. `http://127.0.0.1:8080` |
