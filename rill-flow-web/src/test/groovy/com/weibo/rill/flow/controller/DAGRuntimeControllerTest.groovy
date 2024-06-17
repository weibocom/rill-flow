package com.weibo.rill.flow.controller

import com.weibo.rill.flow.service.facade.DAGRuntimeFacade
import spock.lang.Specification

class DAGRuntimeControllerTest extends Specification {
    DAGRuntimeFacade dagRuntimeFacade = Mock(DAGRuntimeFacade)
    DAGRuntimeController dagRuntimeController = new DAGRuntimeController(dagRuntimeFacade: dagRuntimeFacade)

    def "test completeDAG"() {
        when:
        dagRuntimeFacade.updateDagStatus(*_) >> ret
        then:
        dagRuntimeController.complete(null, "test_execution_id", status) == response
        where:
        status  | ret     | response
        false   | true    | ["code": "0", "message": "success"]
        true    | false   | ["code": "0", "message": "success"]
    }

    def "test completeDAG when throw exception"() {
        when:
        dagRuntimeFacade.updateDagStatus(*_) >> { throw new RuntimeException("test exception message") }
        then:
        dagRuntimeController.complete(null, "test_execution_id", status) == response
        where:
        status | ret     | response
        true   | true    | ["code": "-1", "message": "test exception message"]
    }
}
