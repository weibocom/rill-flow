package com.weibo.rill.flow.service.dispatcher

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.interfaces.model.http.HttpParameter
import com.weibo.rill.flow.interfaces.model.resource.Resource
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic
import org.springframework.http.HttpEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.client.RestClientResponseException
import spock.lang.Specification

class SseProtocolDispatcherTest extends Specification {
    HttpInvokeHelper httpInvokeHelper = Mock(HttpInvokeHelper)
    DAGResourceStatistic dagResourceStatistic = Mock(DAGResourceStatistic)
    SseProtocolDispatcher dispatcher = new SseProtocolDispatcher(sseExecutorHost: "http://rill-flow-sse-executor",
            sseExecutorUri: "/flow/executor/sse/execute.json", httpInvokeHelper: httpInvokeHelper, dagResourceStatistic: dagResourceStatistic)
    Resource resource = new Resource("http://sse-server-url", "sse")

    def setup() {
        httpInvokeHelper.buildUrl(*_) >> "http://sse-server-url"
    }

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
        when:
        HttpEntity<?> httpEntity = dispatcher.buildHttpEntity("123", "task1", resource, headerParam, null, new HashMap<String, Object>())
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
        when:
        HttpEntity<?> httpEntity = dispatcher.buildHttpEntity("123", "task1", resource, headerParam, null, new HashMap<String, Object>())
        then:
        httpEntity.getBody() == ["url": "http://sse-server-url",
                                 "body": ["hello": "world"],
                                 "callback_info": ["trigger_url":"http://callback-server"],
                                 "headers": ["Content-Type":"application/json"],
                                 "request_type": "GET"
        ]
        httpEntity.getHeaders().toSingleValueMap() == ["Content-Type": "application/json", "authHeader": "xxx"]
    }

    def "test buildHttpEntity without callback info"() {
        given:
        HttpParameter requestParams = HttpParameter.builder()
                .queryParams([:])
                .body(["hello": "world"])
                .callback([:])
                .header(["Content-Type":"application/json"]).build()
        httpInvokeHelper.functionRequestParams(*_) >> requestParams
        MultiValueMap<String, String> headerParam = new LinkedMultiValueMap<>()
        headerParam.put("authHeader", ["xxx"])
        when:
        dispatcher.buildHttpEntity("123", "task1", resource, headerParam, null, new HashMap<String, Object>())
        then:
        thrown TaskException
    }

    def "test handle"() {
        given:
        HttpParameter requestParams = HttpParameter.builder()
                .queryParams([:])
                .body(["hello": "world", "callback_info":["trigger_url":"http://callback-server"]])
                .callback([:])
                .header(["Content-Type":"application/json"]).build()
        httpInvokeHelper.functionRequestParams(*_) >> requestParams
        String executionId = "test-execution-id"
        String taskName = "test-task-name"
        TaskInfo taskInfo = Mock(TaskInfo)
        FunctionTask task = Mock(FunctionTask)
        taskInfo.getTask() >> task
        taskInfo.getName() >> taskName
        task.getRequestType() >> null
        MultiValueMap<String, String> headerParam = new LinkedMultiValueMap<>()
        httpInvokeHelper.invokeRequest(*_) >> '{"data": "success"}'
        expect:
        dispatcher.handle(resource, DispatchInfo.builder().executionId(executionId).taskInfo(taskInfo).headers(headerParam).build()) == '{"data": "success"}'
    }

    def "test handle when throw RestClientResponseException"() {
        given:
        HttpParameter requestParams = HttpParameter.builder()
                .queryParams([:])
                .body(["hello": "world", "callback_info":["trigger_url":"http://callback-server"]])
                .callback([:])
                .header(["Content-Type":"application/json"]).build()
        httpInvokeHelper.functionRequestParams(*_) >> requestParams
        TaskInfo taskInfo = Mock(TaskInfo)
        FunctionTask task = Mock(FunctionTask)
        taskInfo.getTask() >> task
        task.getRequestType() >> null
        MultiValueMap<String, String> headerParam = new LinkedMultiValueMap<>()
        httpInvokeHelper.invokeRequest(*_) >> { throw new RestClientResponseException("hello", 500, "internal server error", null, null, null) }
        when:
        dispatcher.handle(resource, DispatchInfo.builder().taskInfo(taskInfo).headers(headerParam).build())
        then:
        thrown TaskException
    }

    def "test handle when throw exception"() {
        given:
        HttpParameter requestParams = HttpParameter.builder()
                .queryParams([:])
                .body(["hello": "world", "callback_info":["trigger_url":"http://callback-server"]])
                .callback([:])
                .header(["Content-Type":"application/json"]).build()
        httpInvokeHelper.functionRequestParams(*_) >> requestParams
        TaskInfo taskInfo = Mock(TaskInfo)
        FunctionTask task = Mock(FunctionTask)
        taskInfo.getTask() >> task
        task.getRequestType() >> null
        MultiValueMap<String, String> headerParam = new LinkedMultiValueMap<>()
        httpInvokeHelper.invokeRequest(*_) >> { throw new RuntimeException() }
        when:
        dispatcher.handle(resource, DispatchInfo.builder().taskInfo(taskInfo).headers(headerParam).build())
        then:
        thrown TaskException
    }
}
