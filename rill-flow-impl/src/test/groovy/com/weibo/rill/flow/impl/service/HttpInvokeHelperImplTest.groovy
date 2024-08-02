package com.weibo.rill.flow.impl.service

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.client.RestClientResponseException
import org.springframework.web.client.RestTemplate
import spock.lang.Specification
import spock.lang.Unroll

class HttpInvokeHelperImplTest extends Specification {
    RestTemplate defaultRestTemplate = Mock(RestTemplate)
    HttpInvokeHelper httpInvokeHelper = new HttpInvokeHelperImpl(defaultRestTemplate: defaultRestTemplate)

    @Unroll
    def "functionRequestParams test"() {
        given:
        String executionId = "xxx"
        String taskInfoName = "testFunctionRequestParams"

        when:
        def httpParameter = httpInvokeHelper.functionRequestParams(executionId, taskInfoName, null, input)

        then:
        httpParameter.queryParams == query
        httpParameter.body == body
        httpParameter.header == header as Map<String, String>

        where:
        input                                                                                                                                         || query                                                                         | body                                                            | header
        ['input_num': 10]                                                                                                                             || ['execution_id': "xxx", 'name': "testFunctionRequestParams"]                  | ['execution_id': "xxx", 'input_num': 10]                        | [:]
        ['body': ['input_num': 10]]                                                                                                                   || ['execution_id': "xxx", 'name': "testFunctionRequestParams"]                  | ['execution_id': "xxx", 'input_num': 10]                        | [:]
        ['query': ['input_num': 10]]                                                                                                                  || ['execution_id': "xxx", 'name': "testFunctionRequestParams", 'input_num': 10] | ['execution_id': "xxx",]                                        | [:]
        ['query': ['user': 'Bob'], 'header': ['content-type': 'application/x-www-form-urlencoded'], 'body': ['title': 'first post']]                  || ['execution_id': "xxx", 'name': "testFunctionRequestParams", 'user': 'Bob']   | ['execution_id': "xxx", 'title': 'first post']                  | ['content-type': 'application/x-www-form-urlencoded']
        ['input_num': 10, 'query': ['user': 'Bob'], 'header': ['content-type': 'application/x-www-form-urlencoded'], 'body': ['title': 'first post']] || ['execution_id': "xxx", 'name': "testFunctionRequestParams", 'user': 'Bob']   | ['execution_id': "xxx", 'input_num': 10, 'title': 'first post'] | ['content-type': 'application/x-www-form-urlencoded']
    }

    @Unroll
    def "test invokeRequest"() {
        when:
        ResponseEntity<String> responseEntity = new ResponseEntity<>("response body", HttpStatus.OK)
        HttpEntity<?> requestEntity = new HttpEntity<>(null, null)
        defaultRestTemplate.exchange(*_) >> responseEntity
        defaultRestTemplate.postForEntity(*_) >> responseEntity
        then:
        httpInvokeHelper.invokeRequest("testExecutionId", "testTaskName",
                "http://localhost:8080/testurl", requestEntity, HttpMethod.GET, 1) == "response body"
        httpInvokeHelper.invokeRequest("testExecutionId", "testTaskName",
                "http://localhost:8080/testurl", requestEntity, HttpMethod.POST, 1) == "response body"
    }

    @Unroll
    def "test invokeRequest throw exception"() {
        given:
        HttpEntity<?> requestEntity = new HttpEntity<>(null, null)
        defaultRestTemplate.exchange(*_) >> { throw new RestClientResponseException("Bad Gateway Timeout", 504, "Bad Gateway Timeout", null, null, null) }
        when:
        httpInvokeHelper.invokeRequest("testExecutionId", "testTaskName",
                "http://localhost:8080/testurl", requestEntity, HttpMethod.GET, 1)
        then:
        thrown(TaskException.class)
    }
}
