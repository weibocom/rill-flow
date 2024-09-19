package com.weibo.rill.flow.service.dispatcher

import com.weibo.rill.flow.interfaces.model.http.HttpParameter
import com.weibo.rill.flow.interfaces.model.resource.Resource
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic
import org.springframework.http.HttpEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Specification

class SseProtocolDispatcherTest extends Specification {
    HttpInvokeHelper httpInvokeHelper = Mock(HttpInvokeHelper)
    DAGResourceStatistic dagResourceStatistic = Mock(DAGResourceStatistic)
    SseProtocolDispatcher dispatcher = new SseProtocolDispatcher(sseExecutorHost: "http://rill-flow-sse-executor",
            sseExecutorUri: "/flow/executor/sse/execute.json", httpInvokeHelper: httpInvokeHelper, dagResourceStatistic: dagResourceStatistic)

    def "test buildUrl"() {
        expect:
        dispatcher.buildUrl("123", "task1") == "http://rill-flow-sse-executor/flow/executor/sse/execute.json?execution_id=123&task_name=task1"
    }

    def "test getName"() {
        expect:
        dispatcher.getName() == "sse"
    }

    def "test buildHttpEntity"() {
        given:
        HttpParameter requestParams = HttpParameter.builder()
                .queryParams([:])
                .body(["hello": "world", "callback_info":["trigger_url":"http://callback-server"]])
                .callback([:])
                .header(["Content-Type":"application/json"]).build()
        httpInvokeHelper.functionRequestParams(*_) >> requestParams
        MultiValueMap<String, String> headerParam = new LinkedMultiValueMap<>()
        headerParam.put("authHeader", ["xxx"])
        httpInvokeHelper.buildUrl(*_) >> "http://sse-server-url"
        when:
        HttpEntity<?> httpEntity = dispatcher.buildHttpEntity("123", "task1", new Resource("http://sse-server-url", "sse"), headerParam, null, new HashMap<String, Object>())
        then:
        httpEntity.getBody() == ["url": "http://sse-server-url",
                                 "body": ["hello": "world", "callback_info":["trigger_url":"http://callback-server"]],
                                 "callback_info": ["trigger_url":"http://callback-server"],
                                 "headers": ["Content-Type":"application/json"],
                                 "request_type": "GET"
        ]
        httpEntity.getHeaders().toSingleValueMap() == ["Content-Type": "application/json", "authHeader": "xxx"]
    }

    def "test buildHttpEntity extract callback_info from header"() {
        given:
        HttpParameter requestParams = HttpParameter.builder()
                .queryParams([:])
                .body(["hello": "world"])
                .callback([:])
                .header(["Content-Type":"application/json"]).build()
        httpInvokeHelper.functionRequestParams(*_) >> requestParams
        MultiValueMap<String, String> headerParam = new LinkedMultiValueMap<>()
        headerParam.put("authHeader", ["xxx"])
        headerParam.put("X-Callback-Url", ["http://callback-server"])
        httpInvokeHelper.buildUrl(*_) >> "http://sse-server-url"
        when:
        HttpEntity<?> httpEntity = dispatcher.buildHttpEntity("123", "task1", new Resource("http://sse-server-url", "sse"), headerParam, null, new HashMap<String, Object>())
        then:
        httpEntity.getBody() == ["url": "http://sse-server-url",
                                 "body": ["hello": "world"],
                                 "callback_info": ["trigger_url":"http://callback-server"],
                                 "headers": ["Content-Type":"application/json"],
                                 "request_type": "GET"
        ]
        httpEntity.getHeaders().toSingleValueMap() == ["Content-Type": "application/json", "authHeader": "xxx", "X-Callback-Url": "http://callback-server"]
    }
}
