package com.weibo.rill.flow.service.facade

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.common.model.User
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.traversal.Olympicene
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException
import com.weibo.rill.flow.service.context.DAGContextInitializer
import com.weibo.rill.flow.service.dconfs.BizDConfs
import com.weibo.rill.flow.service.invoke.DAGFlowRedo
import com.weibo.rill.flow.service.manager.DescriptorManager
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker
import com.weibo.rill.flow.service.statistic.ProfileRecordService
import com.weibo.rill.flow.service.statistic.SystemMonitorStatistic
import com.weibo.rill.flow.service.storage.LongTermStorage
import com.weibo.rill.flow.service.storage.RuntimeStorage
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.lang3.tuple.Pair
import spock.lang.Specification

class OlympiceneFacadeTest extends Specification {
    OlympiceneFacade facade = new OlympiceneFacade()
    ProfileRecordService profileRecordService = new ProfileRecordService()
    DAGSubmitChecker submitChecker = Mock(DAGSubmitChecker)
    DAGContextInitializer dagContextInitializer = new DAGContextInitializer()
    Olympicene olympicene = Mock(Olympicene)
    BizDConfs bizDConfs = Mock(BizDConfs)
    DAGStringParser dagStringParser = Mock(DAGStringParser)
    DescriptorManager descriptorManager = Mock(DescriptorManager)
    DAGResourceStatistic dagResourceStatistic = Mock(DAGResourceStatistic)
    RuntimeStorage runtimeStorage = Mock(RuntimeStorage)
    LongTermStorage longTermStorage = Mock(LongTermStorage)
    DAG dag = new DAG()
    SystemMonitorStatistic systemMonitorStatistic = Mock(SystemMonitorStatistic)
    DAGFlowRedo dagFlowRedo = Mock(DAGFlowRedo)

    def setup() {
        facade.profileRecordService = profileRecordService
        facade.dagSubmitChecker = submitChecker
        facade.dagContextInitializer = dagContextInitializer
        facade.olympicene = olympicene
        facade.dagStringParser = dagStringParser
        facade.descriptorManager = descriptorManager
        facade.dagResourceStatistic = dagResourceStatistic
        facade.runtimeStorage = runtimeStorage
        facade.longTermStorage = longTermStorage
        facade.systemMonitorStatistic = systemMonitorStatistic
        facade.dagFlowRedo = dagFlowRedo
        dagContextInitializer.bizDConfs = bizDConfs

        submitChecker.getCheckConfig(_) >> null
        submitChecker.check(*_) >> null
        descriptorManager.getDagDescriptor(*_) >> null
        dagStringParser.parse(_) >> dag
    }

    def "test submit"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 10240
        expect:
        facade.submit(1L, "testBusiness:testFeatureName", new JSONObject(["resourceName": "testCallbackUrl"]).toJSONString(), null, new JSONObject(["a": 1]), null)
        facade.submit(1L, "testBusiness:testFeatureName", ["resourceName": "testCallbackUrl"], null, null)
        facade.submit(new FlowUser(1L), "testBusiness:testFeatureName", ["resourceName": "testCallbackUrl"], null, null)
    }

    def "test submit exception by limit max context size"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 0
        when:
        facade.submit(1L, "testBusiness:testFeatureName", new JSONObject(["resourceName": "testCallbackUrl"]).toJSONString(), null, new JSONObject(["a": 1]), null)
        then:
        thrown TaskException
    }

    def "test finish"() {
        given:
        olympicene.finish(*_) >> null
        dagResourceStatistic.updateUrlTypeResourceStatus(*_) >> null
        expect:
        ["result": "ok"] == facade.finish("testExecutionId", ["context": ["a":1]], new JSONObject(["passthrough": ["task_name": "testTask"], "result_type": "SUCCESS"]))
    }

    def "test wakeup"() {
        given:
        olympicene.wakeup(*_) >> null
        expect:
        ["result": "ok"] == facade.wakeup("testExecutionId", "testTask", ["context": ["a":1]])
    }

    def "test redo"() {
        given:
        olympicene.redo(*_) >> null
        runtimeStorage.getDAGInfo(_) >> new DAGInfo()
        expect:
        ["result": "ok"] == facade.redo("testExecutionId", ["testTask"], null)
    }

    def "test redo when dag cannot be found"() {
        given:
        runtimeStorage.getDAGInfo(_) >> null
        longTermStorage.getDAGInfo(_) >> null
        when:
        facade.redo("testExecutionId", ["testTask"], null)
        then:
        thrown DAGTraversalException
    }

    def "test redo when dag can be found in long term storage"() {
        given:
        olympicene.redo(*_) >> null
        runtimeStorage.getDAGInfo(_) >> null
        longTermStorage.getDAGInfo(_) >> new DAGInfo()
        longTermStorage.getContext(_) >> ["a": 1]
        runtimeStorage.saveDAGInfo(*_) >> null
        runtimeStorage.updateContext(*_) >> null
        expect:
        ["result": "ok"] == facade.redo("testExecutionId", ["testTask"], null)
    }

    def "test olympicene redo error"() {
        given:
        olympicene.redo(*_) >> {throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "redo executionId can not be empty")}
        runtimeStorage.getDAGInfo(_) >> new DAGInfo()
        when:
        facade.redo(null, ["testTask"], null)
        then:
        thrown DAGTraversalException
    }

    def "test multiRedo"() {
        given:
        Pair<String, String> pair = new ImmutablePair<String, String>("testExecutionId", "time")
        systemMonitorStatistic.getExecutionIdsByStatus(*_) >> [pair]
        systemMonitorStatistic.getExecutionIdsByCode(*_) >> [pair]
        dagFlowRedo.redoFlowWithTrafficLimit(*_) >> null
        expect:
        facade.multiRedo("testBusiness:testService", DAGStatus.FAILED, null, 0L, 1, ["testTaskName"], 5)
        facade.multiRedo("testBusiness:testService", DAGStatus.FAILED, "0", 0L, 1, ["testTaskName"], 5)
    }

    class FlowUser implements User {
        private Long uid

        FlowUser(Long uid) {
            this.uid = uid
        }

        @Override
        long getUid() {
            return uid
        }
    }
}
