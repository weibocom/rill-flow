package com.weibo.rill.flow.service.storage.dao

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import spock.lang.Specification

class DAGBusinessDAOTest extends Specification {
    DAGBusinessDAO dagBusinessDAO
    RedisClient redisClient

    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String BUSINESS_ID_KEY = "business_id"

    def setup() {
        redisClient = Mock(RedisClient)
        dagBusinessDAO = new DAGBusinessDAO()
        dagBusinessDAO.redisClient = redisClient
    }

    def "test createBusiness success"() {
        when:
        def result = dagBusinessDAO.createBusiness(VALID_BUSINESS_ID)

        then:
        1 * redisClient.sadd(BUSINESS_ID_KEY, VALID_BUSINESS_ID)
        result == true
        noExceptionThrown()
    }

    def "test createBusiness with invalid businessId"() {
        when:
        dagBusinessDAO.createBusiness(businessId)

        then:
        0 * redisClient.sadd(_, _)
        thrown(TaskException)

        where:
        businessId << [
            null,                   // null值
            "",                     // 空字符串
            "test_business",        // 包含下划线
            "test-business",        // 包含连字符
            "test@business",        // 包含特殊字符
            "test business"         // 包含空格
        ]
    }

    def "test remBusiness success"() {
        when:
        def result = dagBusinessDAO.remBusiness(VALID_BUSINESS_ID)

        then:
        1 * redisClient.srem(BUSINESS_ID_KEY, VALID_BUSINESS_ID)
        result == true
        noExceptionThrown()
    }

    def "test remBusiness with invalid businessId"() {
        when:
        dagBusinessDAO.remBusiness(businessId)

        then:
        0 * redisClient.srem(_, _)
        thrown(TaskException)

        where:
        businessId << [
            null,                   // null值
            "",                     // 空字符串
            "test_business",        // 包含下划线
            "test-business",        // 包含连字符
            "test@business",        // 包含特殊字符
            "test business"         // 包含空格
        ]
    }

    def "test getBusiness success"() {
        given:
        def expectedBusinesses = ["business1", "business2"] as Set

        when:
        def result = dagBusinessDAO.getBusiness()

        then:
        1 * redisClient.smembers(BUSINESS_ID_KEY, BUSINESS_ID_KEY) >> expectedBusinesses
        result == expectedBusinesses
    }

    def "test getBusiness with empty result"() {
        given:
        def emptySet = [] as Set

        when:
        def result = dagBusinessDAO.getBusiness()

        then:
        1 * redisClient.smembers(BUSINESS_ID_KEY, BUSINESS_ID_KEY) >> emptySet
        result == emptySet
    }
}
