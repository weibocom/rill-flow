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

package com.weibo.rill.flow.service.facade

import com.weibo.rill.flow.interfaces.model.task.InvokeTimeInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic
import com.weibo.rill.flow.service.storage.LongTermStorage
import com.weibo.rill.flow.service.storage.RuntimeStorage
import spock.lang.Specification

class DAGRuntimeFacadeTest extends Specification {
    RuntimeStorage runtimeStorage = Mock(RuntimeStorage)
    LongTermStorage longTermStorage = Mock(LongTermStorage)
    TenantTaskStatistic tenantTaskStatistic = Mock(TenantTaskStatistic)
    DAGRuntimeFacade dagRuntimeFacade = new DAGRuntimeFacade(runtimeStorage: runtimeStorage, longTermStorage: longTermStorage, tenantTaskStatistic: tenantTaskStatistic)

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
        dagRuntimeFacade.updateDagStatus("test_execution_id", DAGStatus.KEY_SUCCEED) == true
        dagRuntimeFacade.updateDagStatus("test_execution_id", DAGStatus.FAILED) == true
    }

    def "test updateDagStatus update invoke time info"() {
        given:
        DAGInfo dagInfo = new DAGInfo()
        dagInfo.setDagStatus(DAGStatus.RUNNING)
        dagInfo.setDagInvokeMsg(DAGInvokeMsg.builder().invokeTimeInfos([InvokeTimeInfo.builder().build()]).build())
        runtimeStorage.getBasicDAGInfo(*_) >> dagInfo
        runtimeStorage.saveDAGInfo(*_) >> null
        dagRuntimeFacade.updateDagStatus("test_execution_id", DAGStatus.SUCCEED)
        expect:
        dagInfo.getDagInvokeMsg().getInvokeTimeInfos().size() == 1
        dagInfo.getDagInvokeMsg().getInvokeTimeInfos().get(0).getEndTimeInMillisecond() > 0
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

    def "test getBasicDAGInfo when DAGInfo does not exist in both storages"() {
        given:
        runtimeStorage.getBasicDAGInfo("test_execution_id") >> null
        longTermStorage.getBasicDAGInfo("test_execution_id") >> null

        when:
        dagRuntimeFacade.getBasicDAGInfo("test_execution_id", false)

        then:
        thrown IllegalArgumentException
    }
}
