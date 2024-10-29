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

class DAGFeatureDAOTest extends Specification {
    DAGFeatureDAO dagFeatureDAO
    RedisClient redisClient

    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_FEATURE_NAME = "testfeature456"

    def setup() {
        redisClient = Mock(RedisClient)
        dagFeatureDAO = new DAGFeatureDAO()
        dagFeatureDAO.redisClient = redisClient
    }

    def "test createFeature success"() {
        given:
        def featureKey = "feature_${VALID_BUSINESS_ID}"

        when:
        def result = dagFeatureDAO.createFeature(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * redisClient.sadd(VALID_BUSINESS_ID, featureKey, [VALID_FEATURE_NAME])
        result == true
        noExceptionThrown()
    }

    def "test createFeature with invalid params"() {
        when:
        dagFeatureDAO.createFeature(businessId, featureName)

        then:
        0 * redisClient.sadd(_, _, _)
        thrown(TaskException)

        where:
        businessId          | featureName
        null               | VALID_FEATURE_NAME
        ""                 | VALID_FEATURE_NAME
        "test_business"    | VALID_FEATURE_NAME    // 包含下划线
        "test@business"    | VALID_FEATURE_NAME    // 包含特殊字符
        VALID_BUSINESS_ID  | null
        VALID_BUSINESS_ID  | ""
        VALID_BUSINESS_ID  | "test_feature"        // 包含下划线
        VALID_BUSINESS_ID  | "test@feature"        // 包含特殊字符
        VALID_BUSINESS_ID  | "test feature"        // 包含空格
    }

    def "test remFeature success"() {
        given:
        def featureKey = "feature_${VALID_BUSINESS_ID}"

        when:
        def result = dagFeatureDAO.remFeature(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * redisClient.srem(VALID_BUSINESS_ID, featureKey, [VALID_FEATURE_NAME])
        result == true
        noExceptionThrown()
    }

    def "test remFeature with invalid params"() {
        when:
        dagFeatureDAO.remFeature(businessId, featureName)

        then:
        0 * redisClient.srem(_, _, _)
        thrown(TaskException)

        where:
        businessId          | featureName
        null               | VALID_FEATURE_NAME
        ""                 | VALID_FEATURE_NAME
        "test_business"    | VALID_FEATURE_NAME    // 包含下划线
        "test@business"    | VALID_FEATURE_NAME    // 包含特殊字符
        VALID_BUSINESS_ID  | null
        VALID_BUSINESS_ID  | ""
        VALID_BUSINESS_ID  | "test_feature"        // 包含下划线
        VALID_BUSINESS_ID  | "test@feature"        // 包含特殊字符
        VALID_BUSINESS_ID  | "test feature"        // 包含空格
    }

    def "test getFeature success"() {
        given:
        def featureKey = "feature_${VALID_BUSINESS_ID}"
        def expectedFeatures = ["feature1", "feature2"] as Set

        when:
        def result = dagFeatureDAO.getFeature(VALID_BUSINESS_ID)

        then:
        1 * redisClient.smembers(VALID_BUSINESS_ID, featureKey) >> expectedFeatures
        result == expectedFeatures
    }

    def "test getFeature with empty result"() {
        given:
        def featureKey = "feature_${VALID_BUSINESS_ID}"
        def emptySet = [] as Set

        when:
        def result = dagFeatureDAO.getFeature(VALID_BUSINESS_ID)

        then:
        1 * redisClient.smembers(VALID_BUSINESS_ID, featureKey) >> emptySet
        result == emptySet
    }
}
