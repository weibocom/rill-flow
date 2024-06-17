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
        dagRuntimeController.completeDAG(null, "test_execution_id", status) == response
        where:
        status | ret    | response
        true   | true   | ["ret": "ok"]
        false  | true   | ["ret": "ok"]
        true   | false  | ["ret": "failed"]
    }
}
