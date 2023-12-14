package com.rill.flow.executor.executor

import com.rill.flow.executor.wrapper.ExecutorContext
import spock.lang.Specification

class SampleExecutorTest extends Specification {

    SampleExecutor sampleExecutor = new SampleExecutor()

    def "'apply' result should contains 'executor_tag' field"() {
        expect:
        sampleExecutor.apply(new ExecutorContext('','',[:]))['executor_tag'] == 'executor'
    }

    def "'apply' should throw runtime exception if request body is null"(){
        when:
        sampleExecutor.apply(new ExecutorContext('','',null))

        then:
        thrown(RuntimeException.class)
    }

}
