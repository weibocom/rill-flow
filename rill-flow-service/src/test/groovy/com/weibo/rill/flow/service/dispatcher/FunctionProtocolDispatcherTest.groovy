/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.service.dispatcher

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.interfaces.model.http.HttpParameter
import com.weibo.rill.flow.interfaces.model.resource.Resource
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClientResponseException
import spock.lang.Specification
import spock.lang.Subject

class FunctionProtocolDispatcherTest extends Specification {
    @Subject
    FunctionProtocolDispatcher dispatcher

    HttpInvokeHelper httpInvokeHelper
    DAGResourceStatistic dagResourceStatistic
    SwitcherManager switcherManager

    def setup() {
        httpInvokeHelper = Mock(HttpInvokeHelper)
        dagResourceStatistic = Mock(DAGResourceStatistic)
        switcherManager = Mock(SwitcherManager)
        dispatcher = new FunctionProtocolDispatcher(
                httpInvokeHelper: httpInvokeHelper,
                dagResourceStatistic: dagResourceStatistic,
                switcherManagerImpl: switcherManager
        )
    }

    def "should handle POST request successfully"() {
        given:
        def executionId = "exec-123"
        def taskName = "testTask"
        def resource = Mock(Resource)
        def input = [key: "value"]
        def taskInfo = new TaskInfo(name: taskName, task: new FunctionTask(taskName, null, null, "function", null, false, null, null, null, null, null, null, null, null, null, null, null, null, "POST", false, null, null, null, null, null, null))
        def headers = new LinkedMultiValueMap<String, String>()
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> executionId
            getInput() >> input
            getTaskInfo() >> taskInfo
            getHeaders() >> headers
        }
        def requestParams = Mock(HttpParameter) {
            getHeader() >> [contentType: MediaType.APPLICATION_JSON_VALUE]
        }
        def url = "http://test.com/api"
        def expectedResponse = '{"status": "success"}'

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(executionId, taskName, resource, input) >> requestParams
        1 * httpInvokeHelper.buildUrl(resource, requestParams.queryParams) >> url
        1 * httpInvokeHelper.invokeRequest(executionId, taskName, url, _ as HttpEntity, HttpMethod.POST, 1) >> expectedResponse
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(executionId, taskName, _, expectedResponse)
        result == expectedResponse
    }

    def "should handle GET request successfully"() {
        given:
        def executionId = "exec-123"
        def taskName = "testTask"
        def resource = Mock(Resource)
        def input = [key: "value"]
        def taskInfo = new TaskInfo(name: taskName, task: new FunctionTask(taskName, null, null, "function", null, false, null, null, null, null, null, null, null, null, null, null, null, null, "GET", false, null, null, null, null, null, null))
        def headers = new LinkedMultiValueMap<String, String>()
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> executionId
            getInput() >> input
            getTaskInfo() >> taskInfo
            getHeaders() >> headers
        }
        def requestParams = Mock(HttpParameter)
        def url = "http://test.com/api"
        def expectedResponse = '{"status": "success"}'

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(executionId, taskName, resource, input) >> requestParams
        1 * httpInvokeHelper.buildUrl(resource, requestParams.queryParams) >> url
        1 * httpInvokeHelper.invokeRequest(executionId, taskName, url, _ as HttpEntity, HttpMethod.GET, 1) >> expectedResponse
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(executionId, taskName, _, expectedResponse)
        result == expectedResponse
    }

    def "should handle error response correctly"() {
        given:
        def executionId = "exec-123"
        def taskName = "testTask"
        def resource = Mock(Resource)
        def input = [key: "value"]
        def taskInfo = new TaskInfo(name: taskName, task: new FunctionTask(taskName, null, null, "function", null, false, null, null, null, null, null, null, null, null, null, null, null, null, "POST", false, null, null, null, null, null, null))
        def headers = new LinkedMultiValueMap<String, String>()
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> executionId
            getInput() >> input
            getTaskInfo() >> taskInfo
            getHeaders() >> headers
        }
        def requestParams = Mock(HttpParameter)
        def url = "http://test.com/api"
        def errorResponse = "Error occurred"
        def exception = Mock(RestClientResponseException) {
            getResponseBodyAsString() >> errorResponse
            getRawStatusCode() >> 500
        }

        when:
        dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(executionId, taskName, resource, input) >> requestParams
        1 * httpInvokeHelper.buildUrl(resource, requestParams.queryParams) >> url
        1 * httpInvokeHelper.invokeRequest(executionId, taskName, url, _ as HttpEntity, HttpMethod.POST, 1) >> { throw exception }
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(executionId, taskName, _, errorResponse)
        thrown(TaskException)
    }

    def "should handle form-urlencoded POST request"() {
        given:
        def executionId = "exec-123"
        def taskName = "testTask"
        def resource = Mock(Resource)
        def input = [key: "value"]
        def taskInfo = new TaskInfo(name: taskName, task: new FunctionTask(taskName, null, null, "function", null, false, null, null, null, null, null, null, null, null, null, null, null, null, "POST", false, null, null, null, null, null, null))
        def headers = new LinkedMultiValueMap<String, String>()
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> executionId
            getInput() >> input
            getTaskInfo() >> taskInfo
            getHeaders() >> headers
        }
        def requestParams = Mock(HttpParameter) {
            getBody() >> [stringParam: "test", mapParam: [key: "value"], listParam: ["item1"]]
        }
        def url = "http://test.com/api"
        def expectedResponse = "success"

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(executionId, taskName, resource, input) >> requestParams
        1 * httpInvokeHelper.buildUrl(resource, requestParams.queryParams) >> url
        1 * httpInvokeHelper.invokeRequest(executionId, taskName, url, _ as HttpEntity, HttpMethod.POST, 1) >> expectedResponse
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(executionId, taskName, _, expectedResponse)
        result == expectedResponse
    }
}
