package com.weibo.rill.flow.service.manager

import spock.lang.Specification

class DescriptorManagerTest extends Specification {
    def "ChangeDescriptorIdToBusinessId"() {
        expect:
        DescriptorManager.changeDescriptorIdToBusinessId(descriptorId) == businessId
        where:
        descriptorId                 | businessId
        "business"                   | "business"
        "business:featureName"       | "business"
        "business:featureName:alias" | "business"
    }
}
