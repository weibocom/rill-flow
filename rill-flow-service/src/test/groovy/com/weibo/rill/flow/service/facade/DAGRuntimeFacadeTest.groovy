package com.weibo.rill.flow.service.facade

import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.service.storage.RuntimeStorage
import spock.lang.Specification

class DAGRuntimeFacadeTest extends Specification {
    RuntimeStorage runtimeStorage = Mock(RuntimeStorage)
    DAGRuntimeFacade dagRuntimeFacade = new DAGRuntimeFacade(runtimeStorage: runtimeStorage)

    def setup() {
        runtimeStorage.clearDAGInfo(*_) >> null
        runtimeStorage.clearContext(*_) >> null
    }

    def "test updateDagStatus execution id null"() {
        when:
        dagRuntimeFacade.updateDagStatus(null, null)
        then:
        thrown IllegalArgumentException
    }

    def "test updateDagStatus status is null"() {
        when:
        dagRuntimeFacade.updateDagStatus("test_execution_id", null)
        then:
        thrown IllegalArgumentException
    }

    def "test updateDagStatus execution id not exist"() {
        when:
        runtimeStorage.getBasicDAGInfo(*_) >> null
        dagRuntimeFacade.updateDagStatus("test_execution_id", DAGStatus.SUCCEED)
        then:
        thrown IllegalArgumentException
    }

    def "test updateDagStatus"() {
        given:
        DAGInfo dagInfo = new DAGInfo()
        dagInfo.setDagStatus(DAGStatus.RUNNING)
        runtimeStorage.getBasicDAGInfo(*_) >> dagInfo
        runtimeStorage.saveDAGInfo(*_) >> null
        expect:
        dagRuntimeFacade.updateDagStatus("test_execution_id", DAGStatus.FAILED) == true
    }

    def "test updateDagStatus throw exception"() {
        given:
        DAGInfo dagInfo = new DAGInfo()
        dagInfo.setDagStatus(DAGStatus.SUCCEED)
        runtimeStorage.getBasicDAGInfo(*_) >> dagInfo
        runtimeStorage.saveDAGInfo(*_) >> null
        when:
        dagRuntimeFacade.updateDagStatus("test_execution_id", DAGStatus.SUCCEED)
        then:
        thrown IllegalArgumentException
    }
}
