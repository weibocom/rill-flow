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
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import spock.lang.Specification

class DAGDescriptorDAOTest extends Specification {
    DAGDescriptorDAO dagDescriptorDAO
    RedisClient redisClient
    SwitcherManager switcherManager

    static final String VALID_BUSINESS_ID = "testbusiness123"
    static final String VALID_FEATURE_NAME = "testfeature456"
    static final String VALID_ALIAS = "testalias789"
    static final String VALID_DESCRIPTOR = "test descriptor content"
    static final String VALID_MD5 = "2f3f2929a7a7133a2a52fa344d17b66a" // md5(VALID_DESCRIPTOR)

    def setup() {
        redisClient = Mock(RedisClient)
        switcherManager = Mock(SwitcherManager)
        dagDescriptorDAO = new DAGDescriptorDAO()
        dagDescriptorDAO.redisClient = redisClient
        dagDescriptorDAO.switcherManagerImpl = switcherManager
        dagDescriptorDAO.versionMaxCount = 300
    }

    def "test getDescriptorPO success with cache disabled"() {
        given:
        def descriptorRedisKey = "descriptor_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_${VALID_MD5}"
        def dagDescriptorId = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_${VALID_MD5}"

        when:
        def result = dagDescriptorDAO.getDescriptorPO(dagDescriptorId, descriptorRedisKey, VALID_BUSINESS_ID)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_GET_DESCRIPTOR_FROM_CACHE") >> false
        1 * redisClient.get(VALID_BUSINESS_ID, descriptorRedisKey) >> VALID_DESCRIPTOR
        result.descriptor == VALID_DESCRIPTOR
    }

    def "test getDescriptorPO success with cache enabled"() {
        given:
        def descriptorRedisKey = "descriptor_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_${VALID_MD5}"
        def dagDescriptorId = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_${VALID_MD5}"

        when:
        def result = dagDescriptorDAO.getDescriptorPO(dagDescriptorId, descriptorRedisKey, VALID_BUSINESS_ID)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_GET_DESCRIPTOR_FROM_CACHE") >> true
        1 * redisClient.get(VALID_BUSINESS_ID, descriptorRedisKey) >> VALID_DESCRIPTOR
        result.descriptor == VALID_DESCRIPTOR
    }

    def "test getDescriptorPO with empty descriptor"() {
        given:
        def descriptorRedisKey = "descriptor_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_${VALID_MD5}"
        def dagDescriptorId = "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_${VALID_MD5}"

        when:
        dagDescriptorDAO.getDescriptorPO(dagDescriptorId, descriptorRedisKey, VALID_BUSINESS_ID)

        then:
        1 * switcherManager.getSwitcherState("ENABLE_GET_DESCRIPTOR_FROM_CACHE") >> false
        1 * redisClient.get(VALID_BUSINESS_ID, descriptorRedisKey) >> null
        thrown(TaskException)
    }

    def "test persistDescriptorPO success"() {
        given:
        def descriptorPO = new DescriptorPO(VALID_DESCRIPTOR)
        def versionKey = "version_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_${VALID_ALIAS}"
        def descriptorKey = "descriptor_${VALID_BUSINESS_ID}_${VALID_FEATURE_NAME}_${VALID_MD5}"

        when:
        def result = dagDescriptorDAO.persistDescriptorPO(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, descriptorPO)

        then:
        1 * redisClient.eval(_, VALID_BUSINESS_ID, 
            [versionKey, descriptorKey], 
            { args -> 
                args.get(0) == "300" &&
                args.get(2) instanceof String && // md5
                args.get(3) == VALID_DESCRIPTOR
            }
        )
        result == "${VALID_BUSINESS_ID}:${VALID_FEATURE_NAME}:md5_${VALID_MD5}"
    }

    def "test persistDescriptorPO with version cleanup"() {
        given:
        def descriptorPO = new DescriptorPO(VALID_DESCRIPTOR)
        dagDescriptorDAO.versionMaxCount = 2 // 设置较小的版本数限制以测试清理逻辑

        when:
        dagDescriptorDAO.persistDescriptorPO(VALID_BUSINESS_ID, VALID_FEATURE_NAME, VALID_ALIAS, descriptorPO)

        then:
        1 * redisClient.eval(_, VALID_BUSINESS_ID, _, 
            { args -> args[0] == "2" } // 验证最大版本数参数
        )
    }
}
