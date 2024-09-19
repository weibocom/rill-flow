package com.weibo.rill.flow.service.dispatcher


import spock.lang.Specification

class SseProtocolDispatcherTest extends Specification {
    SseProtocolDispatcher dispatcher = new SseProtocolDispatcher(sseExecutorHost: "http://rill-flow-sse-executor", sseExecutorUri: "/flow/executor/sse/execute.json")

    def "test buildUrl"() {
        expect:
        dispatcher.buildUrl("123", "task1") == "http://rill-flow-sse-executor/flow/executor/sse/execute.json?execution_id=123&task_name=task1"
    }
}
