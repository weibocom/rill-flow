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
import org.apache.commons.lang3.tuple.Pair
import spock.lang.Specification

class DAGAliasDAOTest extends Specification {
    DAGAliasDAO dagAliasDAO
    RedisClient redisClient

    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_FEATURE_NAME = "testfeature456"
    static final String VALID_ALIAS = "testalias789"
    static final String VALID_MD5 = "abc123def456"

    def setup() {
        redisClient = Mock(RedisClient)
        dagAliasDAO = new DAGAliasDAO()
        dagAliasDAO.redisClient = redisClient
    }

    def "test createAlias success"() {
        when:
        def result = dagAliasDAO.createAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.sadd(VALID_BUSINESS_ID, _, _)
        result == true
    }

    def "test createAlias with invalid params"() {
        when:
        dagAliasDAO.createAlias(businessId, featureName, alias)

        then:
        thrown(TaskException)

        where:
        businessId          | featureName         | alias
        ""                 | VALID_FEATURE_NAME  | VALID_ALIAS
        VALID_BUSINESS_ID  | ""                 | VALID_ALIAS
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | ""
        null              | VALID_FEATURE_NAME  | VALID_ALIAS
        VALID_BUSINESS_ID  | null               | VALID_ALIAS
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | null
        "test_business"   | VALID_FEATURE_NAME  | VALID_ALIAS      // 包含下划线
        VALID_BUSINESS_ID  | "test-feature"     | VALID_ALIAS      // 包含连字符
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | "test@alias"     // 包含特殊字符
    }

    def "test remAlias success"() {
        when:
        def result = dagAliasDAO.remAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.srem(VALID_BUSINESS_ID, _, _)
        result == true
    }

    def "test getAlias success"() {
        given:
        def expectedAliases = ["alias1", "alias2"] as Set

        when:
        def result = dagAliasDAO.getAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * redisClient.smembers(VALID_BUSINESS_ID, _) >> expectedAliases
        result == expectedAliases
    }

    def "test getDescriptorRedisKeyByAlias success"() {
        given:
        def md5Set = [VALID_MD5] as Set

        when:
        def result = dagAliasDAO.getDescriptorRedisKeyByAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.zrange(VALID_BUSINESS_ID, _, -1, -1) >> md5Set
        result == "descriptor_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_${VALID_MD5}"
    }

    def "test getDescriptorRedisKeyByAlias with empty result"() {
        given:
        def emptySet = [] as Set

        when:
        dagAliasDAO.getDescriptorRedisKeyByAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.zrange(VALID_BUSINESS_ID, _, -1, -1) >> emptySet
        thrown(TaskException)
    }

    def "test getVersion success"() {
        given:
        def currentTimeMillis = System.currentTimeMillis()
        def redisResult = [
            Pair.of(VALID_MD5, currentTimeMillis.doubleValue()),
            Pair.of("oldermd5", (currentTimeMillis - 1000).doubleValue())
        ] as Set

        when:
        def result = dagAliasDAO.getVersion(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.zrangeWithScores(VALID_BUSINESS_ID, _, 0, -1) >> redisResult
        result.size() == 2
        result[0].descriptor_id == "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_${VALID_MD5}"
        result[0].create_time == currentTimeMillis
        result[1].create_time == currentTimeMillis - 1000
    }

    def "test getVersion with empty result"() {
        given:
        def emptySet = [] as Set

        when:
        def result = dagAliasDAO.getVersion(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.zrangeWithScores(VALID_BUSINESS_ID, _, 0, -1) >> emptySet
        result.isEmpty()
    }
}
