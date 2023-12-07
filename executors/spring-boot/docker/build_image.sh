#!/bin/bash

cp ../target/*.jar spring-boot-executor.jar

docker build -t weibocom/rill-flow-sample:spring-boot-executor .

sh clean.sh