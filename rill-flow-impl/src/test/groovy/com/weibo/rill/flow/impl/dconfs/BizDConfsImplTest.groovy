package com.weibo.rill.flow.impl.dconfs

import com.weibo.rill.flow.impl.configuration.TestConfig
import com.weibo.rill.flow.service.dconfs.BizDConfs
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

@ContextConfiguration(classes = [BizDConfsImpl.class, TestConfig.class])
class BizDConfsImplTest extends Specification {

    @Autowired
    BizDConfs bizDConfs

    def "test getRuntimeSubmitContextMaxSize"() {
        expect:
        bizDConfs.getRuntimeSubmitContextMaxSize() == 1024
    }

    def "test getRuntimeCallbackContextMaxSize"() {
        expect:
        bizDConfs.getRuntimeCallbackContextMaxSize() == 2048
    }

    def "test getRedisBusinessIdToRuntimeSubmitContextMaxSize"() {
        expect:
        bizDConfs.getRedisBusinessIdToRuntimeSubmitContextMaxSize() == ['testBusiness01':4096, 'testBusiness02':8192]
    }

    def "test getRedisBusinessIdToRuntimeCallbackContextMaxSize"() {
        expect:
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() == ['testBusiness01':3188, 'testBusiness02':9216]
    }
}
