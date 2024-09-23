package com.weibo.rill.flow.service.dispatcher


import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import org.springframework.http.HttpEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Specification

class AnswerTaskDispatcherTest extends Specification {
    HttpInvokeHelper httpInvokeHelper = Mock(HttpInvokeHelper)
    SwitcherManager switcherManagerImpl = Mock(SwitcherManager)
    AnswerTaskDispatcher dispatcher = new AnswerTaskDispatcher(sseExecutorHost: "http://rill-flow-sse-executor",
            answerTaskExecuteUri: "/flow/executor/answer/execute.json", httpInvokeHelper: httpInvokeHelper,
            switcherManagerImpl: switcherManagerImpl)

    def setup() {
        httpInvokeHelper.buildUrl(*_) >> "http://sse-server-url"
        switcherManagerImpl.getSwitcherState(*_) >> true
    }

    def "test buildUrl"() {
        expect:
        dispatcher.buildUrl("123", "task1") == "http://rill-flow-sse-executor/flow/executor/answer/execute.json?execution_id=123&task_name=task1"
    }

    def "test buildHttpEntity"() {
        given:
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<>()
        headers.put("authHeader", ["xxx"])
        DispatchInfo dispatchInfo = DispatchInfo.builder().headers(headers).build()
        when:
        HttpEntity<?> httpEntity = dispatcher.buildHttpEntity(dispatchInfo, "Hello World")
        then:
        httpEntity.getBody() == ["expression": "Hello World"]
        httpEntity.getHeaders().toSingleValueMap() == ["Content-Type": "application/json", "authHeader": "xxx"]
    }
}
