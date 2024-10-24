/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
