package com.weibo.rill.flow.olympicene.traversal.mappings

import spock.lang.Specification

class JSONPathInputOutputMappingTest extends Specification {
    JSONPathInputOutputMapping jsonPathInputOutputMapping = new JSONPathInputOutputMapping(rillFlowFunctionTriggerUri: "/flow/trigger.json", serverHost: "http://localhost:8080")

    def "test calculateSourceValue when source is empty"() {
        when:
        String sourceValue = jsonPathInputOutputMapping.calculateSourceValue("123456", "", [:])
        then:
        sourceValue == null
    }

    def "test calculateSourceValue when source is jsonPath"() {
        given:
        Map<String, Object> env = [input:["hello": "world"]]
        when:
        String sourceValue = jsonPathInputOutputMapping.calculateSourceValue("123456", "\$.input.hello", env)
        then:
        sourceValue == "world"
    }

    def "test calculateSourceValue when source is \$.tasks"() {
        given:
        Map<String, Object> env = [input:["hello": "world"]]
        when:
        String sourceValue = jsonPathInputOutputMapping.calculateSourceValue("123456", "\$.tasks.testTaskName.trigger_url?context=%7B%22trans_finish%22%3A%20true%7D", env)
        then:
        sourceValue == "http://localhost:8080/flow/trigger.json?execution_id=123456&task_name=testTaskName&context=%7B%22trans_finish%22%3A%20true%7D"
    }

    def "test calculateSourceValue when source is \$.tasks but without trigger_url"() {
        given:
        Map<String, Object> env = [input:["hello": "world"]]
        when:
        String sourceValue = jsonPathInputOutputMapping.calculateSourceValue("123456", "\$.tasks.testTaskName.xxx?context=%7B%22trans_finish%22%3A%20true%7D", env)
        then:
        sourceValue == null
    }
}
