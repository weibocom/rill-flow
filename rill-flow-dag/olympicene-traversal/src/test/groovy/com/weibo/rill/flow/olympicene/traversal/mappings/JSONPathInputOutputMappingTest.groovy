package com.weibo.rill.flow.olympicene.traversal.mappings

import spock.lang.Specification

class JSONPathInputOutputMappingTest extends Specification {
    JSONPathInputOutputMapping jsonPathInputOutputMapping = new JSONPathInputOutputMapping(rillFlowFunctionTriggerUri: "/flow/trigger.json", serverHost: "http://localhost:8080")

    def "test calculateSourceValue"() {
        given:
        Map<String, Object> env = [:]
    }
}
