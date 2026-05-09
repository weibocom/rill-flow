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

package com.weibo.rill.flow.service.dispatcher

import com.alibaba.fastjson.JSON
import com.weibo.rill.flow.interfaces.model.resource.Resource
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo
import com.weibo.rill.flow.interfaces.model.task.FunctionPattern
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.traversal.Olympicene
import com.weibo.rill.flow.service.dconfs.BizDConfs
import com.weibo.rill.flow.service.service.DAGDescriptorService
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic
import spock.lang.Specification

class FlowProtocolDispatcherTest extends Specification {
    FlowProtocolDispatcher dispatcher
    DAGDescriptorService dagDescriptorService
    BizDConfs bizDConfs
    Olympicene olympicene
    DAGResourceStatistic dagResourceStatistic

    def setup() {
        dispatcher = new FlowProtocolDispatcher()
        dagDescriptorService = Mock(DAGDescriptorService)
        bizDConfs = Mock(BizDConfs)
        olympicene = Mock(Olympicene)
        dagResourceStatistic = Mock(DAGResourceStatistic)

        dispatcher.dagDescriptorService = dagDescriptorService
        dispatcher.bizDConfs = bizDConfs
        dispatcher.olympicene = olympicene
        dispatcher.dagResourceStatistic = dagResourceStatistic
    }

    def "test handle method with valid input"() {
        given:
        def resource = Mock(Resource) {
            getSchemeValue() >> "test-scheme"
            getResourceName() >> "test-resource"
        }
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getPattern() >> FunctionPattern.FLOW_ASYNC
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "parent-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["uid": "123", "key": "value"]
        }
        def dag = Mock(DAG)

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * bizDConfs.getFlowDAGMaxDepth() >> 10
        1 * dagDescriptorService.getDAG(123L, _, "test-scheme") >> dag
        1 * olympicene.submit(_, dag, _, _, _)
        1 * dagResourceStatistic.updateFlowTypeResourceStatus("parent-execution-id", "test-task", "test-resource", dag)
        
        and:
        def jsonResult = JSON.parseObject(result)
        jsonResult.containsKey("execution_id")
        jsonResult.get("execution_id") != null
    }

    def "test handle method with null input map"() {
        given:
        def resource = Mock(Resource) {
            getSchemeValue() >> "test-scheme"
            getResourceName() >> "test-resource"
        }
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getPattern() >> FunctionPattern.FLOW_ASYNC
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "parent-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> null
        }
        def dag = Mock(DAG)

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * bizDConfs.getFlowDAGMaxDepth() >> 10
        1 * dagDescriptorService.getDAG(0L, _, "test-scheme") >> dag
        1 * olympicene.submit(_, dag, _, _, _)
        1 * dagResourceStatistic.updateFlowTypeResourceStatus("parent-execution-id", "test-task", "test-resource", dag)
        
        and:
        def jsonResult = JSON.parseObject(result)
        jsonResult.containsKey("execution_id")
        jsonResult.get("execution_id") != null
    }

    def "test handle method with invalid uid"() {
        given:
        def resource = Mock(Resource) {
            getSchemeValue() >> "test-scheme"
            getResourceName() >> "test-resource"
        }
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getPattern() >> FunctionPattern.FLOW_ASYNC
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "parent-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["uid": null, "key": "value"]
        }
        def dag = Mock(DAG)

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        1 * bizDConfs.getFlowDAGMaxDepth() >> 10
        1 * dagDescriptorService.getDAG(0L, _, "test-scheme") >> dag
        1 * olympicene.submit(_, dag, _, _, _)
        1 * dagResourceStatistic.updateFlowTypeResourceStatus("parent-execution-id", "test-task", "test-resource", dag)
        
        and:
        def jsonResult = JSON.parseObject(result)
        jsonResult.containsKey("execution_id")
        jsonResult.get("execution_id") != null
    }

    def "test handle method with non-numeric uid"() {
        given:
        def resource = Mock(Resource) {
            getSchemeValue() >> "test-scheme"
            getResourceName() >> "test-resource"
        }
        def taskInfo = Mock(TaskInfo) {
            getName() >> "test-task"
            getTask() >> Mock(FunctionTask) {
                getPattern() >> FunctionPattern.FLOW_ASYNC
            }
        }
        def dispatchInfo = Mock(DispatchInfo) {
            getExecutionId() >> "parent-execution-id"
            getTaskInfo() >> taskInfo
            getInput() >> ["uid": "not-a-number", "key": "value"]
        }
        def dag = Mock(DAG)

        when:
        def result = dispatcher.handle(resource, dispatchInfo)

        then:
        thrown(NumberFormatException)
    }
}
