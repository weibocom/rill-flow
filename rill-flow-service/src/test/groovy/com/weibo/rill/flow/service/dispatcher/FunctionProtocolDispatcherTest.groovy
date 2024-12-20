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
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClientResponseException
import spock.lang.Specification
import spock.lang.Unroll

class FunctionProtocolDispatcherTest extends Specification {
    FunctionProtocolDispatcher dispatcher
    HttpInvokeHelper httpInvokeHelper
    DAGResourceStatistic dagResourceStatistic
    SwitcherManager switcherManager

    def setup() {
        dispatcher = new FunctionProtocolDispatcher()
        httpInvokeHelper = Mock(HttpInvokeHelper)
        dagResourceStatistic = Mock(DAGResourceStatistic)
        switcherManager = Mock(SwitcherManager)

        dispatcher.httpInvokeHelper = httpInvokeHelper
        dispatcher.dagResourceStatistic = dagResourceStatistic
        dispatcher.switcherManagerImpl = switcherManager
    }

    def "test handle method with successful HTTP request"() {
        given:
        def resource = Mock(Resource)
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getRequestType() >> "POST"
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "test-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["key": "value"]
            getHeaders() >> new LinkedMultiValueMap<String, String>()
        }
        def httpParameter = HttpParameter.builder()
                .queryParams([:])
                .body([:])
                .callback([:])
                .header([:])
                .build()
        def expectedResponse = '{"status": "success"}'

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(_, _, _, _) >> httpParameter
        1 * httpInvokeHelper.buildUrl(_, _) >> "http://test.url"
        1 * httpInvokeHelper.invokeRequest(_, _, _, _, _, _) >> expectedResponse
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(_, _, _, expectedResponse)
        result == expectedResponse
    }

    def "test handle method with RestClientResponseException"() {
        given:
        def resource = Mock(Resource) {
            getResourceName() >> "test-resource"
        }
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getRequestType() >> "POST"
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "test-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["key": "value"]
            getHeaders() >> new LinkedMultiValueMap<String, String>()
        }
        def httpParameter = HttpParameter.builder()
                .queryParams([:])
                .body([:])
                .callback([:])
                .header([:])
                .build()
        def errorResponse = "Error response"
        def exception = Mock(RestClientResponseException) {
            getRawStatusCode() >> 500
            getResponseBodyAsString() >> errorResponse
        }

        when:
        dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(_, _, _, _) >> httpParameter
        1 * httpInvokeHelper.buildUrl(_, _) >> "http://test.url"
        1 * httpInvokeHelper.invokeRequest(_, _, _, _, _, _) >> { throw exception }
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(_, _, _, errorResponse)
        thrown(TaskException)
    }

    @Unroll
    def "test handle method with different HTTP methods: #requestType"() {
        given:
        def resource = Mock(Resource)
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getRequestType() >> requestType
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "test-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["key": "value"]
            getHeaders() >> new LinkedMultiValueMap<String, String>()
        }
        def httpParameter = HttpParameter.builder()
                .queryParams([:])
                .body([:])
                .callback([:])
                .header([:])
                .build()
        def expectedResponse = '{"status": "success"}'

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(_, _, _, _) >> httpParameter
        1 * httpInvokeHelper.buildUrl(_, _) >> "http://test.url"
        1 * httpInvokeHelper.invokeRequest(_, _, _, _, expectedMethod, _) >> expectedResponse
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(_, _, _, expectedResponse)
        result == expectedResponse

        where:
        requestType | expectedMethod
        "POST"     | HttpMethod.POST
        "GET"      | HttpMethod.GET
        "PUT"      | HttpMethod.PUT
        null       | HttpMethod.POST  // default method
    }

    def "test handle method with form-urlencoded content type"() {
        given:
        def resource = Mock(Resource)
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getRequestType() >> "POST"
            }
        }
        def headers = new LinkedMultiValueMap<String, String>()
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "test-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["key": "value"]
            getHeaders() >> headers
        }
        def httpParameter = HttpParameter.builder()
                .queryParams([:])
                .body(["formKey": "formValue"])
                .callback([:])
                .header([:])
                .build()
        def expectedResponse = '{"status": "success"}'

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_FUNCTION_DISPATCH_RET_CHECK") >> false
        1 * httpInvokeHelper.functionRequestParams(_, _, _, _) >> httpParameter
        1 * httpInvokeHelper.buildUrl(_, _) >> "http://test.url"
        1 * httpInvokeHelper.invokeRequest(_, _, _, _, _, _) >> expectedResponse
        1 * dagResourceStatistic.updateUrlTypeResourceStatus(_, _, _, expectedResponse)
        result == expectedResponse
    }
}
