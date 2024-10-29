package com.weibo.rill.flow.service.service

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.interfaces.model.resource.BaseResource
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter
import com.weibo.rill.flow.service.storage.dao.DAGABTestDAO
import org.apache.commons.lang3.tuple.Pair
import spock.lang.Specification
import spock.lang.Unroll

class FunctionTaskServiceTest extends Specification {
    FunctionTaskService functionTaskService
    DAGABTestDAO dagABTestDAO
    DAGDescriptorService dagDescriptorService
    DAGDescriptorConverter dagDescriptorConverter

    def setup() {
        dagABTestDAO = Mock(DAGABTestDAO)
        dagDescriptorService = Mock(DAGDescriptorService)
        dagDescriptorConverter = Mock(DAGDescriptorConverter)
        functionTaskService = new FunctionTaskService(
                dagABTestDAO: dagABTestDAO,
                dagDescriptorService: dagDescriptorService,
                dagDescriptorConverter: dagDescriptorConverter
        )
    }

    def "test getTaskResource success"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def resourceName = "dag://descriptor-id?name=resource1"
        def descriptorPO = new DescriptorPO()
        def dag = new DAG()
        def resource = new BaseResource(name: "resource1")
        dag.resources = [resource]

        when:
        def result = functionTaskService.getTaskResource(uid, input, resourceName)

        then:
        1 * dagDescriptorService.getDescriptorPOFromDAO(uid, input, "descriptor-id", true) >> descriptorPO
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> dag
        result == resource
    }

    def "test getTaskResource when resources empty"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def resourceName = "dag://descriptor-id?name=resource1"
        def descriptorPO = new DescriptorPO()
        def dag = new DAG()
        dag.resources = []

        when:
        functionTaskService.getTaskResource(uid, input, resourceName)

        then:
        1 * dagDescriptorService.getDescriptorPOFromDAO(uid, input, "descriptor-id", true) >> descriptorPO
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> dag
        thrown(TaskException)
    }

    def "test getTaskResource when resource not found"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def resourceName = "dag://descriptor-id?name=resource2"
        def descriptorPO = new DescriptorPO()
        def dag = new DAG()
        def resource = new BaseResource(name: "resource1")
        dag.resources = [resource]

        when:
        functionTaskService.getTaskResource(uid, input, resourceName)

        then:
        1 * dagDescriptorService.getDescriptorPOFromDAO(uid, input, "descriptor-id", true) >> descriptorPO
        1 * dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO) >> dag
        thrown(TaskException)
    }

    def "test getTaskResource with invalid URI"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def resourceName = "invalid-uri"

        when:
        functionTaskService.getTaskResource(uid, input, resourceName)

        then:
        thrown(TaskException)
    }

    def "test calculateResourceName success"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def executionId = "business_id:execution_id"
        def configKey = "config_key"
        def abRule = "rule"
        def abRuleMap = [key: "value"]
        def expectedResource = "resource_name"

        when:
        def result = functionTaskService.calculateResourceName(uid, input, executionId, configKey)

        then:
        1 * dagABTestDAO.getFunctionAB(executionId, configKey) >> Pair.of(abRule, abRuleMap)
        1 * dagDescriptorService.getValueFromRuleMap(uid, input, abRuleMap, abRule) >> expectedResource
        result == expectedResource
    }

    def "test calculateResourceName when AB test not found"() {
        given:
        def uid = 123L
        def input = [key: "value"]
        def executionId = "business_id:execution_id"
        def configKey = "config_key"

        when:
        def result = functionTaskService.calculateResourceName(uid, input, executionId, configKey)

        then:
        1 * dagABTestDAO.getFunctionAB(executionId, configKey) >> null
        result == null
    }

    @Unroll
    def "test calculateResourceName with invalid executionId: #executionId"() {
        when:
        functionTaskService.calculateResourceName(123L, [:], executionId, "config_key")

        then:
        noExceptionThrown()
        1 * dagABTestDAO.getFunctionAB(_, _) >> null

        where:
        executionId << ["invalid", ""]
    }
}
