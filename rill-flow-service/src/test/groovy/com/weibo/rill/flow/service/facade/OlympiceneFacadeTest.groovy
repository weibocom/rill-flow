package com.weibo.rill.flow.service.facade

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.common.function.ResourceStatus
import com.weibo.rill.flow.common.model.BusinessHeapStatus
import com.weibo.rill.flow.common.model.User
import com.weibo.rill.flow.interfaces.model.task.BaseTask
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
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
import com.weibo.rill.flow.service.storage.CustomizedStorage
import com.weibo.rill.flow.service.storage.LongTermStorage
import com.weibo.rill.flow.service.storage.RuntimeStorage
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.lang3.tuple.Pair
import spock.lang.Specification

class OlympiceneFacadeTest extends Specification {
    OlympiceneFacade facade = new OlympiceneFacade()
    ProfileRecordService profileRecordService = new ProfileRecordService()
    DAGSubmitChecker dagSubmitChecker = Mock(DAGSubmitChecker)
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
    CustomizedStorage customizedStorage = Mock(CustomizedStorage)

    def setup() {
        facade.profileRecordService = profileRecordService
        facade.dagSubmitChecker = dagSubmitChecker
        facade.dagContextInitializer = dagContextInitializer
        facade.olympicene = olympicene
        facade.dagStringParser = dagStringParser
        facade.descriptorManager = descriptorManager
        facade.dagResourceStatistic = dagResourceStatistic
        facade.runtimeStorage = runtimeStorage
        facade.longTermStorage = longTermStorage
        facade.systemMonitorStatistic = systemMonitorStatistic
        facade.dagFlowRedo = dagFlowRedo
        facade.customizedStorage = customizedStorage
        dagContextInitializer.bizDConfs = bizDConfs

        dagSubmitChecker.getCheckConfig(_) >> null
        dagSubmitChecker.check(*_) >> null
        descriptorManager.getDagDescriptor(*_) >> null
        dagStringParser.parse(_) >> dag
    }

    def "test submit"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 10240
        User user = Mock(User)
        user.getUid() >> 1L
        expect:
        facade.submit(1L, "testBusiness:testFeatureName", new JSONObject(["resourceName": "testCallbackUrl"]).toJSONString(), null, new JSONObject(["a": 1]), null)
        facade.submit(1L, "testBusiness:testFeatureName", ["resourceName": "testCallbackUrl"], null, null)
        facade.submit(user, "testBusiness:testFeatureName", ["resourceName": "testCallbackUrl"], null, null)
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

    def "test getExecutionIdsForBg"() {
        given:
        Pair<String, String> pair1 = new ImmutablePair<String, String>("testExecutionId1", "123")
        Pair<String, String> pair2 = new ImmutablePair<String, String>("testExecutionId2", "456")
        systemMonitorStatistic.getExecutionIdsByStatus(*_) >> [pair1]
        systemMonitorStatistic.getExecutionIdsByCode(*_) >> [pair2]
        when:
        var result1 = facade.getExecutionIdsForBg("testBusiness:testService", DAGStatus.RUNNING, null, 0L, 0, 1)
        var result2 = facade.getExecutionIdsForBg("testBusiness:testService", DAGStatus.RUNNING, "0", 0L, 0, 1)
        then:
        result1.get("execution_ids") == [["execution_id": "testExecutionId1", "submit_time": 123L]]
        result1.get("type") == "running"
        result2.get("execution_ids") == [["execution_id": "testExecutionId2", "submit_time": 456L]]
        result2.get("type") == "0"
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

    def "test taskDegrade"() {
        given:
        DAG dag = Mock(DAG)
        BaseTask task = Mock(BaseTask)
        task.getName() >> "testTaskName"
        dag.getTasks() >> [task]
        runtimeStorage.getDAGDescriptor(_) >> dag
        runtimeStorage.updateDAGDescriptor(*_) >> null
        expect:
        ["result": "ok"] == facade.taskDegrade("testExecutionId", "testTaskName", true, false)
    }

    def "test taskDegrade when task cannot be found"() {
        given:
        DAG dag = Mock(DAG)
        BaseTask task = Mock(BaseTask)
        task.getName() >> "testTaskName1"
        dag.getTasks() >> [task]
        runtimeStorage.getDAGDescriptor(_) >> dag
        runtimeStorage.updateDAGDescriptor(*_) >> null
        when:
        facade.taskDegrade("testExecutionId", "testTaskName2", true, false)
        then:
        thrown TaskException
    }

    def "test businessHeapMonitor"() {
        given:
        JSONObject result = new JSONObject(["a": 1])
        systemMonitorStatistic.businessHeapMonitor(*_) >> result
        expect:
        facade.businessHeapMonitor(["testBusiness:testService"], 0, 100) == result
    }

    def "test getExecutionCount"() {
        given:
        BusinessHeapStatus businessHeapStatus = Mock(BusinessHeapStatus)
        businessHeapStatus.getCollectTime() >> 315L
        systemMonitorStatistic.calculateTimePeriod(*_) >> businessHeapStatus
        systemMonitorStatistic.getExecutionCountByStatus(*_) >> ["dag": "world"]
        systemMonitorStatistic.getExecutionCountByCode(*_) >> ["code": "hello"]
        expect:
        ["code": ["code": "hello"], "dag": ["dag": "world"], "collect_time": 315L] == facade.getExecutionCount("testBusiness:testService", DAGStatus.SUCCEED, "0", 0, 100)
        ["code": ["code": "hello"], "dag": ["dag": "world"], "collect_time": 315L] == facade.getExecutionCount("testBusiness:testService", null, null, 0, 100)
    }

    def "test getExecutionIds by status"() {
        given:
        Pair<String, String> pair1 = new ImmutablePair<String, String>("testExecutionId1", "123")
        Pair<String, String> pair2 = new ImmutablePair<String, String>("testExecutionId2", "456")
        systemMonitorStatistic.getExecutionIdsByStatus(*_) >> [pair1]
        systemMonitorStatistic.getExecutionIdsByCode(*_) >> [pair2]
        when:
        var result = facade.getExecutionIds("testBusiness:testServiceName", DAGStatus.RUNNING, null, 0L, 0, 1)
        then:
        result.get("execution_ids") == [["execution_id": "testExecutionId1", "submit_time": 123L]]
        result.get("type") == "running"
    }

    def "test getExecutionIds by code"() {
        given:
        Pair<String, String> pair1 = new ImmutablePair<String, String>("testExecutionId1", "123")
        Pair<String, String> pair2 = new ImmutablePair<String, String>("testExecutionId2", "456")
        systemMonitorStatistic.getExecutionIdsByStatus(*_) >> [pair1]
        systemMonitorStatistic.getExecutionIdsByCode(*_) >> [pair2]
        when:
        var result = facade.getExecutionIds("testBusiness:testServiceName", DAGStatus.RUNNING, "0", 0L, 0, 1)
        then:
        result.get("execution_ids") == [["execution_id": "testExecutionId2", "submit_time": 456L]]
        result.get("type") == "0"
    }

    def "test statusCheck descriptorId format error"() {
        when:
        facade.statusCheck("hello", null)
        then:
        thrown TaskException
    }

    def "test statusCheck"() {
        given:
        var orderDependentResources = ["testBusiness:testServiceName": ["testResource": ResourceStatus.builder().build()]]
        var submitCheckRet = ["storage_check": true, "resource_check": true, "flow_check": true]
        dagSubmitChecker.getCheckRet(*_) >> submitCheckRet
        dagResourceStatistic.orderDependentResources(*_) >> orderDependentResources
        when:
        var result = facade.statusCheck("testBusiness:testServiceName", null)
        then:
        result == ["descriptor_id": "testBusiness:testServiceName", "submit_status": submitCheckRet, "related_resources": orderDependentResources]
    }

    def "test initBucket"() {
        given:
        customizedStorage.initBucket(*_) >> "testBucketName02"
        expect:
        ["bucket_name": "testBucketName02"] == facade.initBucket("testBucketName01", null)
    }

    def "test storeAndNotify to finish"() {
        given:
        TaskInfo taskInfo = Mock(TaskInfo)
        BaseTask task = Mock(BaseTask)
        taskInfo.getTask() >> task
        task.getCategory() >> "function"
        runtimeStorage.getTaskInfo(*_) >> taskInfo
        customizedStorage.store(*_) >> null
        olympicene.finish(*_) >> null
        dagResourceStatistic.updateUrlTypeResourceStatus(*_) >> null
        expect:
        ["ret": "ok"] == facade.storeAndNotify("testBucketName", null, "testName", new JSONObject(["passthrough": ["task_name": "testTask"], "result_type": "SUCCESS"]).toJSONString())
        ["ret": "ok"] == facade.storeAndNotify("testBucketName", null, null, new JSONObject(["passthrough": ["task_name": "testTask"], "result_type": "SUCCESS"]).toJSONString())
    }

    def "test storeAndNotify to suspense"() {
        given:
        TaskInfo taskInfo = Mock(TaskInfo)
        BaseTask task = Mock(BaseTask)
        taskInfo.getTask() >> task
        task.getCategory() >> "suspense"
        runtimeStorage.getTaskInfo(*_) >> taskInfo
        customizedStorage.store(*_) >> null
        olympicene.wakeup(*_) >> null
        expect:
        ["ret": "ok"] == facade.storeAndNotify("testBucketName", null, "testName", new JSONObject(["passthrough": ["task_name": "testTask"], "result_type": "SUCCESS"]).toJSONString())
    }

    def "test storeAndNotify when category can not be found"() {
        given:
        TaskInfo taskInfo = Mock(TaskInfo)
        BaseTask task = Mock(BaseTask)
        taskInfo.getTask() >> task
        task.getCategory() >> "choice"
        runtimeStorage.getTaskInfo(*_) >> taskInfo
        when:
        facade.storeAndNotify("testBucketName", null, "testName", new JSONObject(["passthrough": ["task_name": "testTask"], "result_type": "SUCCESS"]).toJSONString())
        then:
        thrown TaskException
    }

    def "test load"() {
        given:
        customizedStorage.load(*_) >> ["hello": "world"]
        expect:
        ["bucket_name": "testBucketName", "field_to_value": ["hello": "world"]] == facade.load("testBucketName", false, null, null)
    }

    def "test remove"() {
        given:
        customizedStorage.remove(*_) >> true
        expect:
        ["ret": true] == facade.remove("testBucketName")
        ["ret": true] == facade.remove("testBucketName", ["testField"])
    }
}
