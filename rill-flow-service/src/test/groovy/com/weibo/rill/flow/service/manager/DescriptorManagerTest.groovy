package com.weibo.rill.flow.service.manager

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO
import spock.lang.Specification

class DescriptorManagerTest extends Specification {
    DescriptorManager descriptorManager
    RedisClient redisClient
    SwitcherManager switcherManager
    DAGDescriptorConverter dagDescriptorConverter

    def setup() {
        redisClient = Mock(RedisClient)
        switcherManager = Mock(SwitcherManager)
        dagDescriptorConverter = Mock(DAGDescriptorConverter)
        descriptorManager = new DescriptorManager(
                redisClient: redisClient,
                switcherManagerImpl: switcherManager,
                dagDescriptorConverter: dagDescriptorConverter
        )
    }

    def "test getDagDescriptorPO with valid input"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def dagDescriptorId = "business:feature:alias"
        def descriptorContent = "descriptor content"

        when:
        def result = descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        1 * redisClient.zrange(_, _, -1, -1) >> ["md5hash"]
        1 * redisClient.get(_, _) >> descriptorContent
        result.descriptor == descriptorContent
    }

    def "test getDagDescriptorPO with invalid dagDescriptorId"() {
        given:
        def uid = 123L
        def input = [:]
        def dagDescriptorId = "invalid:id"

        when:
        descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        thrown(TaskException)
    }

    def "test getDagDescriptorPO with empty descriptor"() {
        given:
        def uid = 123L
        def input = [:]
        def dagDescriptorId = "business:feature:alias"

        when:
        descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        thrown(TaskException)
    }

    def "test getDAG with valid input"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def dagDescriptorId = "business:feature:alias"
        def descriptorContent = "descriptor content"
        def descriptorPO = new DescriptorPO(descriptorContent)
        def expectedDAG = new DAG()

        when:
        def result = descriptorManager.getDAG(uid, input, dagDescriptorId)

        then:
        1 * redisClient.zrange(_, _, -1, -1) >> ["md5hash"]
        1 * redisClient.get(_, _) >> descriptorContent
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> expectedDAG
        result == expectedDAG
    }

    def "test getDAG with invalid input"() {
        given:
        def uid = 123L
        def input = [:]
        def dagDescriptorId = "invalid:id"

        when:
        descriptorManager.getDAG(uid, input, dagDescriptorId)

        then:
        thrown(TaskException)
    }

    def "test getDescriptorVO with valid input"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def dagDescriptorId = "business:feature:alias"
        def descriptorContent = "descriptor content"
        def descriptorPO = new DescriptorPO(descriptorContent)
        def expectedDAG = new DAG()
        def expectedDescriptorVO = new DescriptorVO()

        when:
        def result = descriptorManager.getDescriptorVO(uid, input, dagDescriptorId)

        then:
        1 * redisClient.zrange(_, _, -1, -1) >> ["md5hash"]
        1 * redisClient.get(_, _) >> descriptorContent
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> expectedDAG
        1 * dagDescriptorConverter.convertDAGToDescriptorVO(expectedDAG) >> expectedDescriptorVO
        result == expectedDescriptorVO
    }

    def "test getDescriptorVO with invalid input"() {
        given:
        def uid = 123L
        def input = [:]
        def dagDescriptorId = "invalid:id"

        when:
        descriptorManager.getDescriptorVO(uid, input, dagDescriptorId)

        then:
        thrown(TaskException)
    }

    
}
