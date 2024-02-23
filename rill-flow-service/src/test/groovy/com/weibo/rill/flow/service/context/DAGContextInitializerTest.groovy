package com.weibo.rill.flow.service.context

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.service.dconfs.BizDConfs
import com.weibo.rill.flow.service.trace.ContextTraceHook
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class DAGContextInitializerTest extends Specification {
    DAGContextInitializer initializer = new DAGContextInitializer()
    BizDConfs bizDConfs = Mock(BizDConfs)
    ContextTraceHook contextTraceHook = Mock(ContextTraceHook)

    def setup() {
        initializer.bizDConfs = bizDConfs
        contextTraceHook.initialize(_) >> ['hello': 'world']
    }

    def "test newSubmitContextBuilder with data"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> maxSize
        expect:
        result == initializer.newSubmitContextBuilder().withData(data).build()
        where:
        data                        | maxSize   | result
        null                        | 1024      | [:]
        new JSONObject(["a": 1])    | 1024      | ["a": 1]
    }

    def "test newSubmitContextBuilder with hooks"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> maxSize
        contextTraceHook.initialize(_) >> ['hello': 'world']
        expect:
        result == initializer.newSubmitContextBuilder().withData(data).withHooks([contextTraceHook]).build()
        where:
        data                        | maxSize   | result
        new JSONObject(["a": 1])    | 1024      | ["hello": "world"]
    }

    def "test newSubmitContextBuilder throw exception"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 2
        when:
        initializer.newSubmitContextBuilder().withData(new JSONObject(["a": 1])).withHooks(null).build()
        then:
        thrown TaskException
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

    def "test newSubmitContextBuilder when submitContextMaxSizeMap is null"() {
        when:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeSubmitContextMaxSize() >> null
        DAGContextInitializer.DAGContextBuilder builder = initializer.newSubmitContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 1024
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

    def "test newCallbackContextBuilder with CallbackContextMaxSizeMap is null"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() >> null
        DAGContextInitializer.DAGContextBuilder builder = initializer.newCallbackContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 1024
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

    def "test newWakeupContextBuilder with CallbackContextMaxSizeMap is null"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() >> null
        DAGContextInitializer.DAGContextBuilder builder = initializer.newWakeupContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 1024
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

    def "test newRedoContextBuilder with CallbackContextMaxSizeMap is null"() {
        when:
        bizDConfs.getRuntimeCallbackContextMaxSize() >> maxSize
        bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize() >> null
        DAGContextInitializer.DAGContextBuilder builder = initializer.newRedoContextBuilder(businessId)
        then:
        maxContextSize == builder.maxSize
        where:
        maxSize | businessId       | maxContextSize
        1024    | null             | 1024
        1024    | 'testBusiness01' | 1024
    }
}
