package com.weibo.rill.flow.impl.service

import com.google.common.collect.Lists
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.core.model.dag.DAGType
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDAO
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDeserializeService
import com.weibo.rill.flow.olympicene.storage.script.RedisScriptManager
import spock.lang.Specification

class DagInfoDeserializeServiceImplTest extends Specification {
    RedisClient redisClient = Mock(RedisClient)
    DAGInfoDeserializeService dagInfoDeserializeService = new DagInfoDeserializeServiceImpl()
    DAGInfoDAO dagInfoDAO = new DAGInfoDAO(redisClient, dagInfoDeserializeService)
    String executionId = 'executionId'
    DAGInfo dagInfo = new DAGInfo()

    def setup() {
        dagInfo.executionId = executionId
        dagInfo.dagStatus = DAGStatus.NOT_STARTED
        dagInfo.dag = new DAG("workspace", "dagName", "1.0.0", DAGType.FLOW, null, Lists.newArrayList(), null, null, null, null, "ns", "service", null)
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

    def "test get DAGInfo"() {
        given:
        def id = 'xxx'
        redisClient.eval(*_) >> [[["name".getBytes(), "dag_info_xxx".getBytes()], ["@class_execution_id".getBytes(), "java.lang.String".getBytes(), "execution_id".getBytes(), "\"xxx\"".getBytes(),
                                                                                   "@class_dag_status".getBytes(), "com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus".getBytes(), "dag_status".getBytes(), "\"not_started\"".getBytes()]]]
        when:
        def dagInfo = dagInfoDAO.getDagInfo(id, true)

        then:
        noExceptionThrown()
        dagInfo != null
        dagInfo.executionId == 'xxx'
        dagInfo.dagStatus == DAGStatus.NOT_STARTED
    }
}
