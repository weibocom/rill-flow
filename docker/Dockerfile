FROM tomcat:9.0.58-jdk17-openjdk-slim

MAINTAINER Weibo, Inc.

RUN rm -rf /usr/local/tomcat/webapps/*
COPY docker/sources.list /etc/apt/sources.list
COPY docker/opentelemetry-javaagent.jar /usr/local/tomcat/temp/opentelemetry-javaagent.jar

COPY rill-flow-plugins/*/target/*-all.jar /usr/local/rill_flow/plugins/
COPY rill-flow-web/target/rill-flow-web.war /usr/local/tomcat/webapps/ROOT.war
COPY docker/setenv.sh /usr/local/tomcat/bin

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone