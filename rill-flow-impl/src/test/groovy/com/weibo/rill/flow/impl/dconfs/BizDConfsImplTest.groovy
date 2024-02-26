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

    def "test redisBusinessIdToClientId"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToClientId() == ['testBusiness02':'testRedisClient']
    }

    def "test redisServiceIdToClientId"() {
        expect:
        bizDConfsImpl.getRedisServiceIdToClientId() == ['testBusiness01:testFeature01':'testRedisClient']
    }

    def "test redisBusinessIdToFinishReserveSecond"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToFinishReserveSecond() == ['testBusiness01':3600, 'testBusiness02':7200]
    }

    def "test redisBusinessIdToUnfinishedReserveSecond"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToUnfinishedReserveSecond() == ['testBusiness01':3600, 'testBusiness02':7200]
    }

    def "test redisBusinessIdToContextMaxLength"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToContextMaxLength() == ['testBusiness01':1024, 'testBusiness02':2048]
    }

    def "test redisBusinessIdToDAGInfoMaxLength"() {
        expect:
        bizDConfsImpl.getRedisBusinessIdToDAGInfoMaxLength() == ['testBusiness01':1024, 'testBusiness02':2048]
    }

    def "test swapBusinessIdToClientId"() {
        expect:
        bizDConfsImpl.getSwapBusinessIdToClientId() == ['testBusiness01':'testRedisClient']
    }

    def "test swapBusinessIdToFinishReserveSecond"() {
        expect:
        bizDConfsImpl.getSwapBusinessIdToFinishReserveSecond() == ['testBusiness01':3600]
    }

    def "test swapBusinessIdToUnfinishedReserveSecond"() {
        expect:
        bizDConfsImpl.getSwapBusinessIdToUnfinishedReserveSecond() == ['testBusiness01':3600]
    }

    def "test runtimeRedisStorageIdToMaxUsage"() {
        expect:
        bizDConfsImpl.getRuntimeRedisStorageIdToMaxUsage() == ['testRedisClient':85]
    }

    def "test runtimeRedisDefaultStorageMaxUsage"() {
        expect:
        bizDConfsImpl.getRuntimeRedisDefaultStorageMaxUsage() == 85
    }
}
