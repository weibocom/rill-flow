package com.weibo.rill.flow.service.util


import spock.lang.Specification

class ExecutionIdUtilTest extends Specification {
    def "ChangeDescriptorIdToBusinessId"() {
        expect:
        ExecutionIdUtil.changeDescriptorIdToBusinessId(descriptorId) == businessId
        where:
        descriptorId                 | businessId
        "business"                   | "business"
        "business:featureName"       | "business"
        "business:featureName:alias" | "business"
    }
}
