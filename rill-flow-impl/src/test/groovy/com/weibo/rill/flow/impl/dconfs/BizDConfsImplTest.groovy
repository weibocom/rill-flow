package com.weibo.rill.flow.impl.dconfs

import com.weibo.rill.flow.impl.configuration.TestConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

@ContextConfiguration(classes = [BizDConfsImpl.class, TestConfig.class])
class BizDConfsImplTest extends Specification {

    @Autowired
    BizDConfsImpl bizDConfsImpl

    def "test getRuntimeSubmitContextMaxSize"() {
        expect:
        bizDConfsImpl.getRuntimeSubmitContextMaxSize() == 1024
    }

    def "test getRuntimeCallbackContextMaxSize"() {
        expect:
        bizDConfsImpl.getRuntimeCallbackContextMaxSize() == 2048
    }

    def "test getRedisBusinessIdToRuntimeSubmitContextMaxSize"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToRuntimeSubmitContextMaxSize() == ['testBusiness01':4096, 'testBusiness02':8192]
    }

    def "test getRedisBusinessIdToRuntimeCallbackContextMaxSize"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToRuntimeCallbackContextMaxSize() == ['testBusiness01':3188, 'testBusiness02':9216]
    }
}
