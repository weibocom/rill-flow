package com.weibo.rill.flow.service.util

import spock.lang.Specification

class DescriptorIdUtilTest extends Specification {
    def "ChangeDescriptorIdToBusinessId"() {
        expect:
        DescriptorIdUtil.changeDescriptorIdToBusinessId(descriptorId) == businessId
        where:
        descriptorId                 | businessId
        "business"                   | "business"
        "business:featureName"       | "business"
        "business:featureName:alias" | "business"
    }
}
