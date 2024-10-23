package com.weibo.rill.flow.service.strategies

import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import spock.lang.Specification

class DAGProcessStrategyContextTest extends Specification {

    DAGProcessStrategyContext strategyContext
    DAGProcessStrategy defaultStrategy
    DAGProcessStrategy customStrategy

    def setup() {
        strategyContext = new DAGProcessStrategyContext()
        defaultStrategy = Mock(DAGProcessStrategy)
        customStrategy = Mock(DAGProcessStrategy)
        strategyContext.strategies = [
            (DAGProcessStrategyContext.DEFAULT_STRATEGY): defaultStrategy,
            (DAGProcessStrategyContext.CUSTOM_STRATEGY): customStrategy
        ]
    }

    def "onStorage should use the specified strategy when it exists"() {
        given:
        DAG inputDag = new DAG()
        DAG outputDag = new DAG()

        when:
        def result = strategyContext.onStorage(inputDag, DAGProcessStrategyContext.CUSTOM_STRATEGY)

        then:
        1 * customStrategy.onStorage(inputDag) >> outputDag
        result == outputDag
    }

    def "onStorage should use the default strategy when specified strategy doesn't exist"() {
        given:
        DAG inputDag = new DAG()
        DAG outputDag = new DAG()

        when:
        def result = strategyContext.onStorage(inputDag, "nonExistentStrategy")

        then:
        1 * defaultStrategy.onStorage(inputDag) >> outputDag
        result == outputDag
    }

    def "onRetrieval should use the specified strategy when it exists"() {
        given:
        String descriptor = "testDescriptor"
        String processedDescriptor = "processedTestDescriptor"

        when:
        def result = strategyContext.onRetrieval(descriptor, DAGProcessStrategyContext.CUSTOM_STRATEGY)

        then:
        1 * customStrategy.onRetrieval(descriptor) >> processedDescriptor
        result == processedDescriptor
    }

    def "onRetrieval should use the default strategy when specified strategy doesn't exist"() {
        given:
        String descriptor = "testDescriptor"
        String processedDescriptor = "processedTestDescriptor"

        when:
        def result = strategyContext.onRetrieval(descriptor, "nonExistentStrategy")

        then:
        1 * defaultStrategy.onRetrieval(descriptor) >> processedDescriptor
        result == processedDescriptor
    }
}
