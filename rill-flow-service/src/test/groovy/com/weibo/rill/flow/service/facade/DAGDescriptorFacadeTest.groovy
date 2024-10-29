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

package com.weibo.rill.flow.service.facade

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode
import com.weibo.rill.flow.olympicene.storage.exception.StorageException
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter
import com.weibo.rill.flow.service.service.DAGDescriptorService
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker
import com.weibo.rill.flow.service.storage.dao.*
import org.apache.commons.lang3.tuple.Pair
import org.springframework.context.ApplicationEventPublisher
import spock.lang.Specification

class DAGDescriptorFacadeTest extends Specification {
    DAGSubmitChecker dagSubmitChecker
    DAGDescriptorService dagDescriptorService
    ApplicationEventPublisher applicationEventPublisher
    DAGGrayDAO dagGrayDAO
    DAGBusinessDAO dagBusinessDAO
    DAGFeatureDAO dagFeatureDAO
    DAGAliasDAO dagAliasDAO
    DAGABTestDAO dagabTestDAO
    DAGDescriptorConverter dagDescriptorConverter
    DAGDescriptorFacade facade

    // 使用只包含字母和数字的测试数据
    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_FEATURE_NAME = "testfeature456"
    static final String VALID_ALIAS = "testalias789"
    static final String VALID_CONFIG_KEY = "testconfig123"
    static final String VALID_DESCRIPTOR_ID = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_abc123"

    def setup() {
        dagSubmitChecker = Mock(DAGSubmitChecker)
        dagDescriptorService = Mock(DAGDescriptorService)
        applicationEventPublisher = Mock(ApplicationEventPublisher)
        dagGrayDAO = Mock(DAGGrayDAO)
        dagBusinessDAO = Mock(DAGBusinessDAO)
        dagFeatureDAO = Mock(DAGFeatureDAO)
        dagAliasDAO = Mock(DAGAliasDAO)
        dagabTestDAO = Mock(DAGABTestDAO)
        dagDescriptorConverter = Mock(DAGDescriptorConverter)

        facade = new DAGDescriptorFacade(
            dagSubmitChecker: dagSubmitChecker,
            dagDescriptorService: dagDescriptorService,
            applicationEventPublisher: applicationEventPublisher,
            dagGrayDAO: dagGrayDAO,
            dagBusinessDAO: dagBusinessDAO,
            dagFeatureDAO: dagFeatureDAO,
            dagAliasDAO: dagAliasDAO,
            dagabTestDAO: dagabTestDAO,
            dagDescriptorConverter: dagDescriptorConverter
        )
    }

    def "test modifyBusiness success"() {
        when:
        def result = facade.modifyBusiness(add, VALID_BUSINESS_ID)

        then:
        if (add) {
            1 * dagBusinessDAO.createBusiness(VALID_BUSINESS_ID) >> true
        } else {
            1 * dagBusinessDAO.remBusiness(VALID_BUSINESS_ID) >> true
        }
        result == [ret: true]

        where:
        add << [true, false]
    }

    def "test getBusiness success"() {
        given:
        def businesses = ["business1", "business2"] as Set

        when:
        def result = facade.getBusiness()

        then:
        1 * dagBusinessDAO.getBusiness() >> businesses
        result == [business_ids: businesses]
    }

    def "test modifyFeature success"() {
        when:
        def result = facade.modifyFeature(add, VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        if (add) {
            1 * dagBusinessDAO.createBusiness(VALID_BUSINESS_ID)
            1 * dagFeatureDAO.createFeature(VALID_BUSINESS_ID, VALID_FEATURE_NAME) >> true
        } else {
            1 * dagFeatureDAO.remFeature(VALID_BUSINESS_ID, VALID_FEATURE_NAME) >> true
        }
        result == [ret: true]

        where:
        add << [true, false]
    }

    def "test getFeature success"() {
        given:
        def features = ["feature1", "feature2"] as Set

        when:
        def result = facade.getFeature(VALID_BUSINESS_ID)

        then:
        1 * dagFeatureDAO.getFeature(VALID_BUSINESS_ID) >> features
        result == [business_id: VALID_BUSINESS_ID, features: features]
    }

    def "test addDescriptor success"() {
        given:
        def descriptor = "test descriptor content"
        def descriptorId = VALID_DESCRIPTOR_ID

        when:
        def result = facade.addDescriptor(null, VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, descriptor)

        then:
        1 * dagSubmitChecker.checkDAGInfoLengthByBusinessId(VALID_BUSINESS_ID, _)
        1 * dagDescriptorService.saveDescriptorVO(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, _) >> descriptorId
        1 * applicationEventPublisher.publishEvent(_)
        result == [ret: true, descriptor_id: descriptorId]
    }

    def "test addDescriptor when check error"() {
        given:
        def descriptor = "test descriptor content"

        when:
        facade.addDescriptor(null, VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, descriptor)

        then:
        1 * dagSubmitChecker.checkDAGInfoLengthByBusinessId(_, _) >> {
            throw new StorageException(StorageErrorCode.DAG_LENGTH_LIMITATION.getCode(), "DAG length limitation")
        }
        thrown(TaskException)
    }

    def "test getDescriptor success"() {
        given:
        def dag = new DAG()
        def descriptorVO = new DescriptorVO("test descriptor content")
        def input = [uid: "123"]

        when:
        def result = facade.getDescriptor(null, input, VALID_DESCRIPTOR_ID)

        then:
        1 * dagDescriptorService.getDAG(123L, input, VALID_DESCRIPTOR_ID) >> dag
        1 * dagDescriptorConverter.convertDAGToDescriptorVO(dag) >> descriptorVO
        result == [
            descriptor_id: VALID_DESCRIPTOR_ID,
            uid: "123",
            descriptor: descriptorVO.getDescriptor()
        ]
    }

    def "test getFunctionAB success"() {
        given:
        def defaultResource = "defaultResource"
        def rules = [resource1: "rule1", resource2: "rule2"]

        when:
        def result = facade.getFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY)

        then:
        1 * dagabTestDAO.getFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY) >> Pair.of(defaultResource, rules)
        result.business_id == VALID_BUSINESS_ID
        result.config_key == VALID_CONFIG_KEY
        result.ab.default_resource_name == defaultResource
        result.ab.rules.size() == 2
    }

    def "test modifyAlias success"() {
        when:
        def result = facade.modifyAlias(add, VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        if (add) {
            1 * dagAliasDAO.createAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS) >> true
        } else {
            1 * dagAliasDAO.remAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS) >> true
        }
        result == [ret: true]

        where:
        add << [true, false]
    }

    def "test getAlias success"() {
        given:
        def aliases = ["alias1", "alias2"] as Set

        when:
        def result = facade.getAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * dagAliasDAO.getAlias(VALID_BUSINESS_ID, VALID_FEATURE_NAME) >> aliases
        result == [
            business_id: VALID_BUSINESS_ID,
            feature: VALID_FEATURE_NAME,
            aliases: aliases
        ]
    }

    def "test modifyGray success"() {
        given:
        def grayRule = "test gray rule"

        when:
        def result = facade.modifyGray("identity", add, VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, grayRule)

        then:
        if (add) {
            1 * dagGrayDAO.createGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, grayRule) >> true
        } else {
            1 * dagGrayDAO.remGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS) >> true
        }
        result == [ret: true]

        where:
        add << [true, false]
    }

    def "test getGray success"() {
        given:
        def grayRules = [
            "alias1": "rule1",
            "alias2": "rule2"
        ]

        when:
        def result = facade.getGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME)

        then:
        1 * dagGrayDAO.getGray(VALID_BUSINESS_ID, VALID_FEATURE_NAME) >> grayRules
        result == [
            business_id: VALID_BUSINESS_ID,
            feature: VALID_FEATURE_NAME,
            gray: grayRules
        ]
    }

    def "test getABConfigKey success"() {
        given:
        def configKeys = ["key1", "key2"] as Set

        when:
        def result = facade.getABConfigKey(VALID_BUSINESS_ID)

        then:
        1 * dagabTestDAO.getABConfigKey(VALID_BUSINESS_ID) >> configKeys
        result == [
            business_id: VALID_BUSINESS_ID,
            config_keys: configKeys
        ]
    }

    def "test modifyFunctionAB success"() {
        given:
        def defaultResourceName = "defaultResource"
        def abRule = "test ab rule"
        def identity = "testIdentity"

        when:
        def result = facade.modifyFunctionAB(identity, add, VALID_BUSINESS_ID, VALID_CONFIG_KEY, defaultResourceName, abRule)

        then:
        if (add) {
            1 * dagabTestDAO.createFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY, defaultResourceName, abRule) >> true
        } else {
            1 * dagabTestDAO.remFunctionAB(VALID_BUSINESS_ID, VALID_CONFIG_KEY, defaultResourceName) >> true
        }
        1 * applicationEventPublisher.publishEvent(_)
        result == [ret: true]

        where:
        add << [true, false]
    }

    def "test getVersion success"() {
        given:
        def versions = [
            [descriptor_id: VALID_DESCRIPTOR_ID, create_time: 1234567890],
            [descriptor_id: "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_def456", create_time: 1234567891]
        ]

        when:
        def result = facade.getVersion(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS)

        then:
        1 * dagAliasDAO.getVersion(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS) >> versions
        result == [
            business_id: VALID_BUSINESS_ID,
            feature: VALID_FEATURE_NAME,
            alias: VALID_ALIAS,
            versions: versions
        ]
    }

    def "test getDescriptor with yaml content success"() {
        given:
        def yamlContent = """
            tasks:
              - name: task1
                resourceName: resource1
                next: task2,task3
              - name: task2
                resourceName: resource2
              - name: task3
                resourceName: resource3
        """
        def dag = new DAG()
        def descriptorVO = new DescriptorVO(yamlContent)

        when:
        def result = facade.getDescriptor(VALID_DESCRIPTOR_ID)

        then:
        1 * dagDescriptorService.getDAG(null, null, VALID_DESCRIPTOR_ID) >> dag
        1 * dagDescriptorConverter.convertDAGToDescriptorVO(dag) >> descriptorVO
        result instanceof JSONObject
        result.tasks.task1.task.name == "task1"
        result.tasks.task1.next == ["task2", "task3"]
        result.tasks.task2.task.name == "task2"
        result.tasks.task3.task.name == "task3"
    }

    def "test getDescriptor with invalid yaml content"() {
        given:
        def dag = new DAG()
        def descriptorVO = new DescriptorVO("invalid: yaml: content: :")

        when:
        facade.getDescriptor(VALID_DESCRIPTOR_ID)

        then:
        1 * dagDescriptorService.getDAG(null, null, VALID_DESCRIPTOR_ID) >> dag
        1 * dagDescriptorConverter.convertDAGToDescriptorVO(dag) >> descriptorVO
        thrown(TaskException)
    }

    def "test getDescriptor with null descriptor"() {
        given:
        def dag = new DAG()
        def descriptorVO = new DescriptorVO(null)

        when:
        facade.getDescriptor(VALID_DESCRIPTOR_ID)

        then:
        1 * dagDescriptorService.getDAG(null, null, VALID_DESCRIPTOR_ID) >> dag
        1 * dagDescriptorConverter.convertDAGToDescriptorVO(dag) >> descriptorVO
        thrown(TaskException)
    }
}
