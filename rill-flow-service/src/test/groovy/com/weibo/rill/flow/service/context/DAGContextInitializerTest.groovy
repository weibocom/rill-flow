package com.weibo.rill.flow.service.context

import com.weibo.rill.flow.service.dconfs.BizDConfs
import spock.lang.Specification

class DAGContextInitializerTest extends Specification {
    DAGContextInitializer initializer = new DAGContextInitializer()
    BizDConfs bizDConfs = Mock(BizDConfs)

    def setup() {
        initializer.bizDConfs = bizDConfs
    }

    def "test newSubmitContextBuilder"() {
        when:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> maxSize
        DAGContextInitializer.DAGContextBuilder builder = initializer.newSubmitContextBuilder()
        then:
        maxSize == builder.maxSize
        where:
        maxSize << [1, 2, 3]
    }

    def "test newSubmitContextBuilder with businessId"() {
        when:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeSubmitContextMaxSize() >> ['testBusiness01':4096, 'testBusiness02':8192]
        DAGContextInitializer.DAGContextBuilder builder = initializer.newSubmitContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 4096
        1024    | 'testBusiness02' | 8192
        1024    | 'testBusiness03' | 1024
    }

    def "test newCallbackContextBuilder"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        DAGContextInitializer.DAGContextBuilder builder = initializer.newCallbackContextBuilder()
        then:
        maxSize == builder.maxSize
        where:
        maxSize << [1, 2, 3]
    }

    def "test newCallbackContextBuilder with businessId"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() >> ['testBusiness01':3188, 'testBusiness02':9216]
        DAGContextInitializer.DAGContextBuilder builder = initializer.newCallbackContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 3188
        1024    | 'testBusiness02' | 9216
        1024    | 'testBusiness03' | 1024
    }

    def "test newWakeupContextBuilder"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        DAGContextInitializer.DAGContextBuilder builder = initializer.newWakeupContextBuilder()
        then:
        maxSize == builder.maxSize
        where:
        maxSize << [1, 2, 3]
    }

    def "test newWakeupContextBuilder with businessId"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() >> ['testBusiness01':3188, 'testBusiness02':9216]
        DAGContextInitializer.DAGContextBuilder builder = initializer.newWakeupContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 3188
        1024    | 'testBusiness02' | 9216
        1024    | 'testBusiness03' | 1024
    }

    def "test newRedoContextBuilder"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        DAGContextInitializer.DAGContextBuilder builder = initializer.newRedoContextBuilder()
        then:
        maxSize == builder.maxSize
        where:
        maxSize << [1, 2, 3]
    }

    def "test newRedoContextBuilder with businessId"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() >> ['testBusiness01':3188, 'testBusiness02':9216]
        DAGContextInitializer.DAGContextBuilder builder = initializer.newRedoContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 3188
        1024    | 'testBusiness02' | 9216
        1024    | 'testBusiness03' | 1024
    }
}
