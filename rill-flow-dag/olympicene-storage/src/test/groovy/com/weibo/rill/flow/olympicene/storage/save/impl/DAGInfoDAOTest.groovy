package com.weibo.rill.flow.olympicene.storage.save.impl

import com.google.common.collect.Lists
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.core.model.dag.DAGType
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.olympicene.storage.script.RedisScriptManager
import spock.lang.Specification

class DAGInfoDAOTest extends Specification {
    RedisClient redisClient = Mock(RedisClient)
    DAGInfoDeserializeService dagInfoDeserializeService = Mock(DAGInfoDeserializeService)
    DAGInfoDAO dagInfoDAO = new DAGInfoDAO(redisClient, dagInfoDeserializeService)
    String executionId = 'executionId'
    DAGInfo dagInfo = new DAGInfo()

    def setup() {
        dagInfo.executionId = executionId
        dagInfo.dagStatus = DAGStatus.NOT_STARTED
        dagInfo.dag = new DAG("workspace", "dagName", "1.0.0", DAGType.FLOW, null, Lists.newArrayList(), null, null, null, null, "ns", "service", null)
    }

    def "updateDagInfo redis eval shardingKey check"() {
        when:
        dagInfoDAO.updateDagInfo(executionId, dagInfo)

        then:
        noExceptionThrown()
        1 * redisClient.eval(RedisScriptManager.dagInfoSetScript(), "executionId", _, _)
    }

    def "updateDagInfo with empty tasks"() {
        when:
        dagInfoDAO.updateDagInfo(executionId, dagInfo)

        then:
        noExceptionThrown()
        1 * redisClient.eval(RedisScriptManager.dagInfoSetScript(),
                "executionId",
                {
                    List<String> keys ->
                        keys.get(0).startsWith('dag_descriptor_') && keys.get(1) == 'dag_info_executionId'
                },
                {
                    List<String> args ->
                        args.size() == 16 &&
                                args.subList(0, 11) == ['172800', '_placeholder_', '{"workspace":"workspace","dagName":"dagName","version":"1.0.0","type":"flow","tasks":[]}', '_placeholder_', 'execution_id', '"executionId"', 'dag_status', '"not_started"', '@class_dag_status', 'com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus', 'dag'] &&
                                args.get(11).startsWith('"dag_descriptor_') &&
                                args.subList(12, 16) == ['@class_dag', 'java.lang.String', '@class_execution_id', 'java.lang.String']
                }
        )
    }

    def "updateDagInfo with no nested tasks"() {
        given:
        dagInfo.setTask("A", new TaskInfo(name: "A"))

        when:
        dagInfoDAO.updateDagInfo(executionId, dagInfo)

        then:
        noExceptionThrown()
        1 * redisClient.eval(RedisScriptManager.dagInfoSetScript(),
                "executionId",
                {
                    List<String> keys ->
                        keys.get(0).startsWith('dag_descriptor_') && keys.get(1) == 'dag_info_executionId'
                },
                {
                    List<String> args ->
                        args.size() == 20 &&
                                args.subList(0, 13) == ['172800', '_placeholder_', '{"workspace":"workspace","dagName":"dagName","version":"1.0.0","type":"flow","tasks":[]}', '_placeholder_', 'execution_id', '"executionId"', 'dag_status', '"not_started"', '@class_#A', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '@class_dag_status', 'com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus', 'dag'] &&
                                args.get(13).startsWith('"dag_descriptor_') &&
                                args.subList(14, 17) == ['@class_dag', 'java.lang.String', '#A'] &&
                                args.get(17).endsWith('"name":"A","next":[],"children":{},"dependencies":[]}') &&
                                args.subList(18, 20) == ['@class_execution_id', 'java.lang.String']
                }
        )
    }

    def "updateDagInfo with nested tasks"() {
        given:
        TaskInfo taskInfo = new TaskInfo(name: "A")
        taskInfo.setChildren(["A1": new TaskInfo(name: "A1")])
        dagInfo.setTask("A", taskInfo)

        when:
        dagInfoDAO.updateDagInfo(executionId, dagInfo)

        then:
        noExceptionThrown()
        1 * redisClient.eval(RedisScriptManager.dagInfoSetScript(),
                "executionId",
                {
                    List<String> keys ->
                        keys.get(0).startsWith('dag_descriptor_') && keys.subList(1, 4) == ['dag_info_executionId', 'sub_task_executionId_A', 'sub_task_mapping_executionId']
                },
                {
                    List<String> args ->
                        args.size() == 28 &&
                                args.subList(0, 13) == ['172800', '_placeholder_', '{"workspace":"workspace","dagName":"dagName","version":"1.0.0","type":"flow","tasks":[]}', '_placeholder_', 'execution_id', '"executionId"', 'dag_status', '"not_started"', '@class_#A', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '@class_dag_status', 'com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus', 'dag'] &&
                                args.get(13).startsWith('"dag_descriptor_') &&
                                args.subList(14, 17) == ['@class_dag', 'java.lang.String', '#A'] &&
                                args.get(17).endsWith('"name":"A","next":[],"children":{},"dependencies":[]}') &&
                                args.subList(18, 24) == ['@class_execution_id', 'java.lang.String', '_placeholder_', '@class_#A1', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '#A1'] &&
                                args.get(24).endsWith('"name":"A1","next":[],"children":{},"dependencies":[]}') &&
                                args.subList(25, 28) == ['_placeholder_', 'A', 'sub_task_executionId_A']
                }
        )
    }

    def "saveTaskInfos support ancestor sub nested tasks"() {
        given:
        Set<TaskInfo> taskInfos = []
        taskInfos.add(new TaskInfo(name: "A"))
        taskInfos.add(new TaskInfo(name: "B_0-B1"))
        TaskInfo taskInfo = new TaskInfo(name: "C")
        taskInfo.setChildren(["C1": new TaskInfo(name: "C1")])
        taskInfos.add(taskInfo)

        when:
        dagInfoDAO.saveTaskInfos(executionId, taskInfos)

        then:
        noExceptionThrown()
        1 * redisClient.eval(RedisScriptManager.dagInfoSetScript(),
                "executionId",
                ['dag_info_executionId', 'sub_task_executionId_B', 'sub_task_executionId_C', 'sub_task_mapping_executionId'],
                {
                    List<String> args ->
                        args.size() == 25 &&
                                args.subList(0, 3) == ['172800', '_placeholder_', '#C'] &&
                                args.subList(4, 9) == ['@class_#A', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '@class_#C', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '#A'] &&
                                args.subList(10, 12) == ['_placeholder_', '#B_0-B1'] &&
                                args.subList(13, 17) == ['@class_#B_0-B1', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '_placeholder_', '#C1'] &&
                                args.subList(18, 25) == ['@class_#C1', 'com.weibo.rill.flow.interfaces.model.task.TaskInfo', '_placeholder_', 'B', 'sub_task_executionId_B', 'C', 'sub_task_executionId_C']
                }
        )
    }

    def "delDagInfo invoke setting if time above zero"() {
        given:
        DAGInfoDAO dagInfoDAOMock = Spy(DAGInfoDAO, constructorArgs: [redisClient, dagInfoDeserializeService]) as DAGInfoDAO
        dagInfoDAOMock.getFinishStatusReserveTimeInSecond(*_) >> reserveTime

        when:
        dagInfoDAOMock.delDagInfo(executionId)

        then:
        invokeTime * redisClient.eval(RedisScriptManager.getRedisExpire(),
                "executionId",
                ['dag_info_executionId', 'sub_task_mapping_executionId'],
                [String.valueOf(reserveTime)])

        where:
        reserveTime | invokeTime
        -1          | 0
        0           | 1
        1           | 1
    }

    def "getDagInfoFromRedis param needSubTasks decide keys value"() {
        when:
        dagInfoDAO.getDagInfoFromRedis(executionId, needSubTask)

        then:
        1 * redisClient.eval(RedisScriptManager.dagInfoGetScript(), executionId, keys, Lists.newArrayList())

        where:
        needSubTask | keys
        true        | ["dag_info_executionId", "sub_task_mapping_executionId"]
        false       | ["dag_info_executionId"]
    }
}
