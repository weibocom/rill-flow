package com.weibo.rill.flow.impl.service


import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import spock.lang.Specification
import spock.lang.Unroll

class HttpInvokeHelperImplTest extends Specification {
    HttpInvokeHelper httpInvokeHelper = new HttpInvokeHelperImpl()

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
}
