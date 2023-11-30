package com.weibo.rill.flow.olympicene.storage.save.impl

import spock.lang.Specification

class DAGRedisStorageTest extends Specification {
    DAGInfoDAO dagInfoDAO = Mock(DAGInfoDAO.class, constructorArgs: [null, null]) as DAGInfoDAO
    ContextDAO contextDao = Mock(ContextDAO.class, constructorArgs: [null]) as ContextDAO
    DAGRedisStorage dagRedisStorage = new DAGRedisStorage(dagInfoDAO, contextDao)

    def "getDAGInfo should return sub tasks"() {
        when:
        dagRedisStorage.getDAGInfo("executionId")

        then:
        1 * dagInfoDAO.getDagInfo("executionId", true)
    }

    def "getNakedDAGInfo should not return sub tasks"() {
        when:
        dagRedisStorage.getBasicDAGInfo("executionId")

        then:
        1 * dagInfoDAO.getDagInfo("executionId", false)
    }

    def "getNakedTaskInfo should not return sub tasks"() {
        when:
        dagRedisStorage.getBasicTaskInfo("executionId", "taskName")

        then:
        1 * dagInfoDAO.getBasicTaskInfo("executionId", "taskName")
    }

    def "getTaskInfo should return sub tasks"() {
        when:
        dagRedisStorage.getTaskInfo("executionId", "taskName")

        then:
        1 * dagInfoDAO.getTaskInfoWithAllSubTask("executionId", "taskName")
    }

    def "getContext should not return sub context"() {
        when:
        dagRedisStorage.getContext("executionId")

        then:
        1 * contextDao.getContext("executionId", false)
    }
}
