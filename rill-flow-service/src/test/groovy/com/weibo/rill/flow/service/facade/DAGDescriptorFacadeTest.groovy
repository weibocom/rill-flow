package com.weibo.rill.flow.service.facade

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode
import com.weibo.rill.flow.olympicene.storage.exception.StorageException
import com.weibo.rill.flow.service.manager.DescriptorManager
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker
import org.springframework.context.ApplicationEventPublisher
import spock.lang.Specification

class DAGDescriptorFacadeTest extends Specification {
    DAGSubmitChecker dagSubmitChecker = Mock(DAGSubmitChecker)
    DescriptorManager descriptorManager = Mock(DescriptorManager)
    ApplicationEventPublisher applicationEventPublisher = Mock(ApplicationEventPublisher)
    DAGDescriptorFacade facade = new DAGDescriptorFacade(
            dagSubmitChecker: dagSubmitChecker,
            applicationEventPublisher: applicationEventPublisher,
            descriptorManager: descriptorManager
    )

    def "test addDescriptor"() {
        given:
        var descriptor_id = "testBusiness:testFeatureName_c_8921a32f"
        applicationEventPublisher.publishEvent(*_) >> null
        dagSubmitChecker.checkDAGInfoLengthByBusinessId(*_) >> null
        descriptorManager.createDAGDescriptor(*_) >> descriptor_id
        expect:
        facade.addDescriptor(null, "testBusiness", "testFeatureName", "release", "hello world") == [ret: true, descriptor_id: descriptor_id]
        facade.addDescriptor(null, "testBusiness", "testFeatureName", "release", "") == [ret: true, descriptor_id: descriptor_id]
    }

    def "test addDescriptor when check error"() {
        given:
        var descriptor_id = "testBusiness:testFeatureName_c_8921a32f"
        applicationEventPublisher.publishEvent(*_) >> null
        dagSubmitChecker.checkDAGInfoLengthByBusinessId(*_) >> {throw new StorageException(StorageErrorCode.DAG_LENGTH_LIMITATION.getCode(), "DAG length limitation")}
        descriptorManager.createDAGDescriptor(*_) >> descriptor_id
        when:
        facade.addDescriptor(null, "testBusiness", "testFeatureName", "release", "hello world")
        then:
        thrown TaskException
    }

    def "test generateResourceProtocol"() {
        given:
        JSONObject task = new JSONObject(["resourceProtocol": "testProtocol", "resourceName": "http://rill-flow-server"])
        when:
        facade.generateResourceProtocol(task)
        then:
        task != null
        task.getString("resourceProtocol") == "testProtocol"
    }

    def "test generateResourceProtocol without protocol"() {
        given:
        JSONObject task = new JSONObject(["resourceName": "http://rill-flow-server"])
        when:
        facade.generateResourceProtocol(task)
        then:
        task != null
        task.getString("resourceProtocol") == "http"
    }
}
