package com.weibo.rill.flow.olympicene.storage.save.impl

import com.google.common.collect.Lists
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode
import com.weibo.rill.flow.olympicene.storage.exception.StorageException
import com.weibo.rill.flow.olympicene.storage.script.RedisScriptManager
import spock.lang.Specification

class ContextDAOTest extends Specification {
    RedisClient redisClient = Mock(RedisClient.class)
    ContextDAO contextDAO = new ContextDAO(redisClient)
    ContextDAO contextDAOMock = Spy(ContextDAO.class, constructorArgs: [redisClient]) as ContextDAO
    String executionId = 'executionId'

    def "getContextNameToContentMap subContext value must be map"() {
        when:
        contextDAO.getContextNameToContentMap(1, "root", ["__A": "value"])

        then:
        def e = thrown(StorageException)
        e.getErrorCode() == StorageErrorCode.CLASS_TYPE_NONSUPPORT.getCode()
    }

    def "updateContext support subContext"() {
        given:
        Map<String, Object> context = ["A": "url", "B": ["list"], "C": ["key": "value"], "__D": ["subKey": "value", "__E": ["subSubKey": "value"]]]

        when:
        contextDAO.updateContext(executionId, context)

        then:
        1 * redisClient.eval(RedisScriptManager.getRedisSetWithExpire(),
                'executionId',
                ['context_executionId', 'sub_context_executionId___E', 'sub_context_executionId___D', 'context_mapping_executionId'],
                ['172800', '_placeholder_', '@class_C', 'java.util.LinkedHashMap', 'A', '"url"', '@class_@subContextName___D', 'java.lang.String', 'B', '["list"]', 'C', '{"key":"value"}', '@subContextName___D', '"__D"', '@class_A', 'java.lang.String', '@class_B', 'java.util.ArrayList', '_placeholder_', 'subSubKey', '"value"', '@class_subSubKey', 'java.lang.String', '_placeholder_', '@class_@subContextName___E', 'java.lang.String', '@class_subKey', 'java.lang.String', 'subKey', '"value"', '@subContextName___E', '"__E"', '_placeholder_', '__E', 'sub_context_executionId___E', '__D', 'sub_context_executionId___D']
        )
    }

    def "deleteContext invoke setting if time above zero"() {
        given:
        contextDAOMock.getFinishStatusReserveTimeInSecond(*_) >> reserveTime

        when:
        contextDAOMock.deleteContext(executionId)

        then:
        invokeTime * redisClient.eval(RedisScriptManager.getRedisExpire(),
                "executionId",
                ['context_executionId', 'context_mapping_executionId'],
                [String.valueOf(reserveTime)])

        where:
        reserveTime | invokeTime
        -1          | 0
        0           | 1
        1           | 1
    }

    def "checkContextLength throws no exception when not check content or meet length"() {
        given:
        contextDAOMock.enableContextLengthCheck(*_) >> enableCheck
        contextDAOMock.contextMaxLength(*_) >> maxLength

        when:
        contextDAOMock.checkContextLength("executionId", [[1, 2, 3] as byte[]])

        then:
        noExceptionThrown()

        where:
        enableCheck | maxLength
        false       | _
        true        | 6
    }

    def "checkContextLength throws exception when check content and not meet length"() {
        given:
        contextDAOMock.enableContextLengthCheck(*_) >> true
        contextDAOMock.contextMaxLength(*_) >> 1

        when:
        contextDAOMock.checkContextLength("executionId", [[1, 2, 3] as byte[]])

        then:
        def exception = thrown(StorageException)
        exception.getErrorCode() == StorageErrorCode.CONTEXT_LENGTH_LIMITATION.getCode()
    }

    def "getContextFromRedis param needSubTasks decide keys value"() {
        when:
        contextDAO.getContextFromRedis(executionId, needSubContext)

        then:
        1 * redisClient.eval(RedisScriptManager.getRedisGet(), executionId, keys, Lists.newArrayList())

        where:
        needSubContext | keys
        true           | ['context_executionId', 'context_mapping_executionId']
        false          | ['context_executionId']
    }

    def "distinguishField check"() {
        when:
        List<String> rootContextFields = []
        List<String> subContextNames = []
        contextDAO.distinguishField(["A", "B", "__C"], rootContextFields, subContextNames)

        then:
        rootContextFields == ["A", "B"]
        subContextNames == ["__C"]
    }

    def "buildEvalParam build redis eval keys and argv by fields"() {
        when:
        List<String> keys = []
        List<String> argv = []
        contextDAO.buildEvalParam(executionId, rootFields, subFields, keys, argv)

        then:
        keys == keysValue
        argv == argvValue

        where:
        rootFields | subFields      | keysValue                                                                             | argvValue
        ["A", "B"] | ["__C", "__D"] | ["context_executionId", "sub_context_executionId___C", "sub_context_executionId___D"] | ["A", "@class_A", "B", "@class_B"]
        []         | ["__C", "__D"] | ["sub_context_executionId___C", "sub_context_executionId___D"]                        | []
        ["A", "B"] | []             | ["context_executionId"]                                                               | ["A", "@class_A", "B", "@class_B"]
    }
}
