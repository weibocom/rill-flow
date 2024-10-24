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
