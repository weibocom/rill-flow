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

package com.weibo.rill.flow.service.service

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter
import com.weibo.rill.flow.service.manager.AviatorCache
import com.weibo.rill.flow.service.storage.dao.*
import spock.lang.Specification

class DAGDescriptorServiceTest extends Specification {
    DAGDescriptorService service
    DAGAliasDAO dagAliasDAO
    DAGFeatureDAO dagFeatureDAO
    DAGDescriptorDAO dagDescriptorDAO
    DAGDescriptorConverter dagDescriptorConverter
    DAGGrayDAO dagGrayDAO
    DAGBusinessDAO dagBusinessDAO
    AviatorCache aviatorCache

    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_FEATURE_NAME = "testfeature456"
    static final String VALID_ALIAS = "testalias789"
    static final String VALID_DESCRIPTOR_ID = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:${VALID_ALIAS}"
    static final String VALID_MD5_DESCRIPTOR_ID = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_abc123"
    static final String VALID_REDIS_KEY = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:abc123"

    def setup() {
        dagAliasDAO = Mock(DAGAliasDAO)
        dagFeatureDAO = Mock(DAGFeatureDAO)
        dagDescriptorDAO = Mock(DAGDescriptorDAO)
        dagDescriptorConverter = Mock(DAGDescriptorConverter)
        dagGrayDAO = Mock(DAGGrayDAO)
        dagBusinessDAO = Mock(DAGBusinessDAO)
        aviatorCache = Mock(AviatorCache)

        service = new DAGDescriptorService(
            dagAliasDAO: dagAliasDAO,
            dagFeatureDAO: dagFeatureDAO,
            dagDescriptorDAO: dagDescriptorDAO,
            dagDescriptorConverter: dagDescriptorConverter,
            dagGrayDAO: dagGrayDAO,
            dagBusinessDAO: dagBusinessDAO,
            aviatorCache: aviatorCache
        )
    }

    def "test getDAG success with alias"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def descriptorPO = new DescriptorPO()
        def dag = new DAG()

        when:
        def result = service.getDAG(uid, input, VALID_DESCRIPTOR_ID)

        then:
        0 * dagGrayDAO.getGray(_, _)
        1 * dagAliasDAO.getDescriptorRedisKeyByAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS) >> VALID_REDIS_KEY
        1 * dagDescriptorDAO.getDescriptorPO(VALID_DESCRIPTOR_ID, VALID_REDIS_KEY, VALID_BUSINESS_ID) >> descriptorPO
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> dag
        result == dag
    }

    def "test getDAG success with gray rules"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def descriptorPO = new DescriptorPO()
        def dag = new DAG()
        def descriptorId = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}"
        def grayRules = [
            (VALID_ALIAS): "uid > 100"
        ]

        when:
        def result = service.getDAG(uid, input, descriptorId)

        then:
        1 * dagGrayDAO.getGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME) >> grayRules
        1 * aviatorCache.getAviatorExpression("uid > 100") >> Mock(com.googlecode.aviator.Expression) {
            execute(_) >> true
        }
        1 * dagAliasDAO.getDescriptorRedisKeyByAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS) >> VALID_REDIS_KEY
        1 * dagDescriptorDAO.getDescriptorPO(descriptorId, VALID_REDIS_KEY, VALID_BUSINESS_ID) >> descriptorPO
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> dag
        result == dag
    }

    def "test getDAG success with md5 descriptor id"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def descriptorPO = new DescriptorPO()
        def dag = new DAG()
        def md5DescriptorId = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_abc123"
        def redisKey = "descriptor_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_abc123"

        when:
        def result = service.getDAG(uid, input, md5DescriptorId)

        then:
        0 * dagGrayDAO.getGray(_, _)
        0 * dagAliasDAO.getDescriptorRedisKeyByAlias(_, _, _)
        1 * dagDescriptorDAO.getDescriptorPO(md5DescriptorId, redisKey, VALID_BUSINESS_ID) >> descriptorPO
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> dag
        result == dag
    }

    def "test getDAG with invalid descriptor id"() {
        when:
        service.getDAG(123L, [:], descriptorId)

        then:
        thrown(TaskException)

        where:
        descriptorId << [
            null,                        // 空值
            "",                          // 空字符串
            "invalid",                   // 缺少分隔符
            "invalid@business@feature",  // 包含非法字符
            ":",                         // 空字段
            "::",                        // 多个空字段
            "test:",                     // 缺少第二个字段
            ":test"                      // 缺少第一个字段
        ]
    }

    def "test getDAG with invalid business id or feature name format"() {
        given:
        def invalidDescriptorId = "${businessId}:${featureName}"

        when:
        service.getDAG(123L, [:], invalidDescriptorId)

        then:
        thrown(TaskException)

        where:
        businessId          | featureName
        "invalid@business"  | "feature"           // 非法字符在 businessId
        "business"          | "invalid@feature"   // 非法字符在 featureName
        "a"                 | "feature"           // businessId 太短
        "business"          | "a"                 // featureName 太短
        "b" * 129          | "feature"           // businessId 太长
        "business"          | "f" * 129          // featureName 太长
    }

    def "test getDAG with valid format but nonexistent descriptor"() {
        given:
        def descriptorId = "validbusiness:validfeature"  // 格式正确但不存在的描述符

        when:
        service.getDAG(123L, [:], descriptorId)

        then:
        1 * dagGrayDAO.getGray("validbusiness", "validfeature") >> [:]
        1 * dagAliasDAO.getDescriptorRedisKeyByAlias("validbusiness", "validfeature", "release") >> null
    }

    def "test saveDescriptorVO success"() {
        given:
        def descriptorVO = new DescriptorVO("test content")
        def dag = new DAG(workspace: VALID_BUSINESS_ID, dagName: VALID_FEATURE_NAME)
        def descriptorPO = new DescriptorPO()

        when:
        def result = service.saveDescriptorVO(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, descriptorVO)

        then:
        1 * dagDescriptorConverter.convertDescriptorVOToDAG(descriptorVO) >> dag
        1 * dagBusinessDAO.createBusiness(VALID_BUSINESS_ID)
        1 * dagFeatureDAO.createFeature(VALID_BUSINESS_ID, VALID_FEATURE_NAME)
        1 * dagAliasDAO.createAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)
        1 * dagDescriptorConverter.convertDAGToDescriptorPO(dag) >> descriptorPO
        1 * dagDescriptorDAO.persistDescriptorPO(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, descriptorPO) >> VALID_MD5_DESCRIPTOR_ID
        result == VALID_MD5_DESCRIPTOR_ID
    }

    def "test saveDescriptorVO with invalid params"() {
        when:
        service.saveDescriptorVO(businessId, featureName, alias, descriptorVO)

        then:
        thrown(TaskException)

        where:
        businessId          | featureName         | alias          | descriptorVO
        null               | VALID_FEATURE_NAME  | VALID_ALIAS    | new DescriptorVO("test")
        ""                 | VALID_FEATURE_NAME  | VALID_ALIAS    | new DescriptorVO("test")
        "invalid_business" | VALID_FEATURE_NAME  | VALID_ALIAS    | new DescriptorVO("test")
        VALID_BUSINESS_ID  | null               | VALID_ALIAS    | new DescriptorVO("test")
        VALID_BUSINESS_ID  | ""                 | VALID_ALIAS    | new DescriptorVO("test")
        VALID_BUSINESS_ID  | "invalid_feature"  | VALID_ALIAS    | new DescriptorVO("test")
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | null          | new DescriptorVO("test")
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | ""            | new DescriptorVO("test")
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | "invalid_alias"| new DescriptorVO("test")
        VALID_BUSINESS_ID  | VALID_FEATURE_NAME  | VALID_ALIAS    | null
    }
}
