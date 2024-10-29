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

package com.weibo.rill.flow.service.storage.dao

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import spock.lang.Specification

class DAGABTestDAOTest extends Specification {
    DAGABTestDAO dagABTestDAO
    RedisClient redisClient

    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_CONFIG_KEY = "testconfig456"
    static final String VALID_RESOURCE = "testresource789"
    static final String VALID_RULE = "testruleabc"

    def setup() {
        redisClient = Mock(RedisClient)
        dagABTestDAO = new DAGABTestDAO()
        dagABTestDAO.redisClient = redisClient
    }

    def "test createABConfigKey success"() {
        when:
        dagABTestDAO.createABConfigKey(VALID_BUSINESS_ID, VALID_CONFIG_KEY)

        then:
        1 * redisClient.sadd(VALID_BUSINESS_ID, _, _)
        noExceptionThrown()
    }

    def "test createABConfigKey with invalid params"() {
        when:
        dagABTestDAO.createABConfigKey(businessId, configKey)

        then:
        thrown(TaskException)

        where:
        businessId          | configKey
        ""                 | VALID_CONFIG_KEY
        VALID_BUSINESS_ID  | ""
        null              | VALID_CONFIG_KEY
        VALID_BUSINESS_ID  | null
        "test_business"   | VALID_CONFIG_KEY    // 包含下划线
        VALID_BUSINESS_ID  | "test@config"      // 包含特殊字符
        "test-business"   | VALID_CONFIG_KEY    // 包含连字符
    }

    def "test getABConfigKey success"() {
        given:
        def expectedKeys = ["key1", "key2"] as Set

        when:
        def result = dagABTestDAO.getABConfigKey(VALID_BUSINESS_ID)

        then:
        1 * redisClient.smembers(VALID_BUSINESS_ID, _) >> expectedKeys
        result == expectedKeys
    }

    def "test createFunctionAB success with default rule"() {
        when:
        def result = dagABTestDAO.createFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY, VALID_RESOURCE, "default")

        then:
        1 * redisClient.sadd(VALID_BUSINESS_ID, _, _)
        1 * redisClient.hmset(VALID_BUSINESS_ID, _, _)
        result == true
    }

    def "test createFunctionAB with non-default rule after default resource exists"() {
        given:
        def mockRedisResult = ["defaultsomeresource": "default"]

        when:
        def result = dagABTestDAO.createFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY, VALID_RESOURCE, VALID_RULE)

        then:
        1 * redisClient.sadd(VALID_BUSINESS_ID, _, _)
        1 * redisClient.hgetAll(VALID_BUSINESS_ID, _) >> mockRedisResult
        1 * redisClient.hmset(VALID_BUSINESS_ID, _, _)
        result == true
    }

    def "test createFunctionAB with non-default rule without default resource should fail"() {
        given:
        def emptyRedisResult = [:]

        when:
        dagABTestDAO.createFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY, VALID_RESOURCE, VALID_RULE)

        then:
        1 * redisClient.sadd(VALID_BUSINESS_ID, _, _)
        1 * redisClient.hgetAll(VALID_BUSINESS_ID, _) >> emptyRedisResult
        thrown(TaskException)
    }

    def "test remFunctionAB success"() {
        when:
        def result = dagABTestDAO.remFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY, VALID_RESOURCE)

        then:
        1 * redisClient.hdel(VALID_BUSINESS_ID, _, _)
        result == true
    }

    def "test getFunctionAB success"() {
        given:
        def mockRedisResult = [
            "defaulttestresource789": "default",
            "otherresource123": VALID_RULE
        ]

        when:
        def result = dagABTestDAO.getFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY)

        then:
        1 * redisClient.hgetAll(VALID_BUSINESS_ID, _) >> mockRedisResult
        result.left == "testresource789"
        result.right == ["otherresource123": VALID_RULE]
    }

    def "test getFunctionAB with empty result"() {
        given:
        def emptyRedisResult = [:]

        when:
        def result = dagABTestDAO.getFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY)

        then:
        1 * redisClient.hgetAll(VALID_BUSINESS_ID, _) >> emptyRedisResult
        result.left == null
        result.right.isEmpty()
    }
}
