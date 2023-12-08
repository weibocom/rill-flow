# Java Executor
Rill Flow provides a Java executor, allowing users to customize business processing logic using the SDK.

## Build
Users can use the provided Rill Flow [packaging script](executor-web/docker/build_image.sh) to create a Docker image for the Java executor.
```shell
sh build_image.sh
```

## Deployment
Users can modify the [executor Kubernetes deployment YAML](../../docs/samples/executor/deploy-sample-executor.yaml) provided by Rill Flow and deploy the executor on a Kubernetes cluster using the Kubernetes deployment command.

Alternatively, modify the docker-compose file:
```shell
cat <<EOF > docker-compose-sample.yaml
version: '3'
services:
  sample-executor:
    image: weibocom/rill-flow-sample:spring-boot-executor
EOF
docker-compose up -d
```

## Invoke
Invoke the executor using the Rill Flow [HTTP dispatcher](https://rill-flow.github.io/docs/user-guide/defination/task-and-dispatcher#http-协议派发器).