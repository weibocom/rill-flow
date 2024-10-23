package com.weibo.rill.flow.service.strategies

import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import spock.lang.Specification

class DefaultDAGProcessStrategyTest extends Specification {
    
    def defaultStrategy = new DefaultDAGProcessStrategy()

    def "test onStorage method"() {
        given:
        def inputDAG = Mock(DAG)

        when:
        def result = defaultStrategy.onStorage(inputDAG)

        then:
        result == inputDAG
    }

    def "test onRetrieval method"() {
        given:
        def inputDescriptor = "test descriptor"

        when:
        def result = defaultStrategy.onRetrieval(inputDescriptor)

        then:
        result == inputDescriptor
    }
}
