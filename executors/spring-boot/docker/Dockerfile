FROM openjdk:17

MAINTAINER Weibo, Inc.

COPY spring-boot-executor.jar /app/

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["java", "-jar", "/app/spring-boot-executor.jar"]