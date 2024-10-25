package com.weibo.rill.flow.service.manager

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO
import org.apache.commons.codec.digest.DigestUtils
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
        Map<String, Object> input = [:]
        def dagDescriptorId = "invalid:id"

        when:
        descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        thrown(TaskException)
    }

    def "test getDagDescriptorPO with empty descriptor"() {
        given:
        def uid = 123L
        Map<String, Object> input = [:]
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
        Map<String, Object> input = [:]
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
        Map<String, Object> input = [:]
        def dagDescriptorId = "invalid:id"

        when:
        descriptorManager.getDescriptorVO(uid, input, dagDescriptorId)

        then:
        thrown(TaskException)
    }

    def "test getDagDescriptorPO with dagDescriptorId having only one field"() {
        given:
        def uid = 123L
        Map<String, Object> input = [:]
        def dagDescriptorId = "business"

        when:
        descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        thrown(TaskException)
    }

    def "test getDagDescriptorPO with invalid business or feature name"() {
        given:
        def uid = 123L
        Map<String, Object> input = [:]
        def dagDescriptorId = "business:feature!"

        when:
        descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        thrown(TaskException)
    }

    def "test getDagDescriptorPO with empty third field"() {
        given:
        def uid = 123L
        Map<String, Object> input = [:]
        def dagDescriptorId = "business:feature"
        def descriptorContent = "descriptor content"

        when:
        def result = descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        1 * redisClient.hgetAll(_, _) >> [release: "true"]
        1 * redisClient.zrange(_, _, -1, -1) >> ["md5hash"]
        1 * redisClient.get(_, _) >> descriptorContent
        result.descriptor == descriptorContent
    }

    def "test getDagDescriptorPO with MD5 prefix in third field"() {
        given:
        long uid = 123L
        Map<String, Object> input = [:]
        String md5 = "1234567890abcdef"
        String dagDescriptorId = "business:feature:md5_${md5}"
        String descriptorContent = "descriptor content"

        when:
        def result = descriptorManager.getDagDescriptorPO(uid, input, dagDescriptorId, false)

        then:
        1 * redisClient.get(_, _) >> descriptorContent
        result.descriptor == descriptorContent
    }

    def "test createDAGDescriptor with valid input"() {
        given:
        def businessId = "testBusiness"
        def featureName = "testFeature"
        def alias = "testAlias"
        def descriptorVO = new DescriptorVO()
        def dag = new DAG(workspace: businessId, dagName: featureName)
        def descriptorPO = new DescriptorPO("test descriptor content")
        def md5 = DigestUtils.md5Hex(descriptorPO.getDescriptor())

        when:
        def result = descriptorManager.createDAGDescriptor(businessId, featureName, alias, descriptorVO)

        then:
        1 * dagDescriptorConverter.convertDescriptorVOToDAG(descriptorVO) >> dag
        1 * dagDescriptorConverter.convertDAGToDescriptorPO(dag) >> descriptorPO
        1 * redisClient.eval(_, businessId, _, _) >> "OK"
        result == "${businessId}:${featureName}:md5_${md5}"
    }

    def "test createDAGDescriptor with null descriptorVO"() {
        given:
        def businessId = "testBusiness"
        def featureName = "testFeature"
        def alias = "testAlias"

        when:
        descriptorManager.createDAGDescriptor(businessId, featureName, alias, null)

        then:
        thrown(TaskException)
    }

    def "test createDAGDescriptor with invalid businessId"() {
        given:
        def businessId = "invalid business"
        def featureName = "testFeature"
        def alias = "testAlias"
        def descriptorVO = new DescriptorVO()

        when:
        descriptorManager.createDAGDescriptor(businessId, featureName, alias, descriptorVO)

        then:
        thrown(TaskException)
    }

    def "test createDAGDescriptor with mismatched businessId"() {
        given:
        def businessId = "testBusiness"
        def featureName = "testFeature"
        def alias = "testAlias"
        def descriptorVO = new DescriptorVO()
        def dag = new DAG(workspace: "differentBusiness", dagName: featureName)

        when:
        descriptorManager.createDAGDescriptor(businessId, featureName, alias, descriptorVO)

        then:
        1 * dagDescriptorConverter.convertDescriptorVOToDAG(descriptorVO) >> dag
        thrown(TaskException)
    }

    def "test createDAGDescriptor with mismatched featureName"() {
        given:
        def businessId = "testBusiness"
        def featureName = "testFeature"
        def alias = "testAlias"
        def descriptorVO = new DescriptorVO()
        def dag = new DAG(workspace: businessId, dagName: "differentFeature")

        when:
        descriptorManager.createDAGDescriptor(businessId, featureName, alias, descriptorVO)

        then:
        1 * dagDescriptorConverter.convertDescriptorVOToDAG(descriptorVO) >> dag
        thrown(TaskException)
    }
}
