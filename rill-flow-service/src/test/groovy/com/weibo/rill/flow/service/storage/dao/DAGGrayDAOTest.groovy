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

class DAGGrayDAOTest extends Specification {
    DAGGrayDAO dagGrayDAO
    RedisClient redisClient

    // 使用只包含字母和数字的测试数据
    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_FEATURE_NAME = "testfeature456"
    static final String VALID_ALIAS = "testalias789"
    static final String VALID_GRAY_RULE = "gray rule content"

    def setup() {
        redisClient = Mock(RedisClient)
        dagGrayDAO = new DAGGrayDAO()
        dagGrayDAO.redisClient = redisClient
    }

    def "test createGray success"() {
        given:
        def grayKey = "gray_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}"

        when:
        def result = dagGrayDAO.createGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, VALID_GRAY_RULE)

        then:
        1 * redisClient.hmset(VALID_BUSINESS_ID, grayKey, [(VALID_ALIAS): VALID_GRAY_RULE])
        result == true
        noExceptionThrown()
    }

    def "test createGray with invalid params"() {
        when:
        dagGrayDAO.createGray(businessId, featureName, alias, grayRule)

        then:
        0 * redisClient.hmset(_, _, _)
        thrown(TaskException)

        where:
        businessId          | featureName         | alias          | grayRule
        null               | VALID_FEATURE_NAME  | VALID_ALIAS    | VALID_GRAY_RULE
        ""                 | VALID_FEATURE_NAME  | VALID_ALIAS    | VALID_GRAY_RULE
        "test_business"    | VALID_FEATURE_NAME  | VALID_ALIAS    | VALID_GRAY_RULE    // 包含下划线
        VALID_BUSINESS_ID  | null               | VALID_ALIAS    | VALID_GRAY_RULE
        VALID_BUSINESS_ID  | ""                 | VALID_ALIAS    | VALID_GRAY_RULE
        VALID_BUSINESS_ID  | "test_feature"     | VALID_ALIAS    | VALID_GRAY_RULE    // 包含下划线
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | null          | VALID_GRAY_RULE
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | ""            | VALID_GRAY_RULE
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | "test_alias"  | VALID_GRAY_RULE    // 包含下划线
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | VALID_ALIAS    | null
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | VALID_ALIAS    | ""
    }

    def "test remGray success"() {
        given:
        def grayKey = "gray_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}"

        when:
        def result = dagGrayDAO.remGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * redisClient.hdel(VALID_BUSINESS_ID, grayKey, [VALID_ALIAS])
        result == true
        noExceptionThrown()
    }

    def "test remGray with invalid params"() {
        when:
        dagGrayDAO.remGray(businessId, featureName, alias)

        then:
        0 * redisClient.hdel(_, _, _)
        thrown(TaskException)

        where:
        businessId          | featureName         | alias
        null               | VALID_FEATURE_NAME  | VALID_ALIAS
        ""                 | VALID_FEATURE_NAME  | VALID_ALIAS
        "test_business"    | VALID_FEATURE_NAME  | VALID_ALIAS    // 包含下划线
        VALID_BUSINESS_ID  | null               | VALID_ALIAS
        VALID_BUSINESS_ID  | ""                 | VALID_ALIAS
        VALID_BUSINESS_ID  | "test_feature"     | VALID_ALIAS    // 包含下划线
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | null
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | ""
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | "test_alias"  // 包含下划线
    }

    def "test getGray success"() {
        given:
        def grayKey = "gray_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}"
        def expectedGrayRules = [
            "alias1": "rule1",
            "alias2": "rule2"
        ]

        when:
        def result = dagGrayDAO.getGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * redisClient.hgetAll(VALID_BUSINESS_ID, grayKey) >> expectedGrayRules
        result == expectedGrayRules
    }

    def "test getGray with empty result"() {
        given:
        def grayKey = "gray_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}"
        def emptyMap = [:]

        when:
        def result = dagGrayDAO.getGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * redisClient.hgetAll(VALID_BUSINESS_ID, grayKey) >> emptyMap
        result == emptyMap
    }
}
