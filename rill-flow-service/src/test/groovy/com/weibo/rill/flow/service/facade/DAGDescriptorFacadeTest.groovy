package com.weibo.rill.flow.service.facade

import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode
import com.weibo.rill.flow.olympicene.storage.exception.StorageException
import com.weibo.rill.flow.service.service.DAGDescriptorService
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker
import org.springframework.context.ApplicationEventPublisher
import spock.lang.Specification

class DAGDescriptorFacadeTest extends Specification {
    DAGSubmitChecker dagSubmitChecker = Mock(DAGSubmitChecker)
    DAGDescriptorService dagDescriptorService = Mock(DAGDescriptorService)
    ApplicationEventPublisher applicationEventPublisher = Mock(ApplicationEventPublisher)
    DAGDescriptorFacade facade = new DAGDescriptorFacade(
            dagSubmitChecker: dagSubmitChecker,
            applicationEventPublisher: applicationEventPublisher,
            dagDescriptorService: dagDescriptorService
    )

    def "test addDescriptor"() {
        given:
        var descriptor_id = "testBusiness:testFeatureName_c_8921a32f"
        applicationEventPublisher.publishEvent(*_) >> null
        dagSubmitChecker.checkDAGInfoLengthByBusinessId(*_) >> null
        dagDescriptorService.saveDescriptorVO(*_) >> descriptor_id
        expect:
        facade.addDescriptor(null, "testBusiness", "testFeatureName", "release", "hello world") == [ret: true, descriptor_id: descriptor_id]
        facade.addDescriptor(null, "testBusiness", "testFeatureName", "release", "") == [ret: true, descriptor_id: descriptor_id]
    }

    def "test addDescriptor when check error"() {
        given:
        var descriptor_id = "testBusiness:testFeatureName_c_8921a32f"
        applicationEventPublisher.publishEvent(*_) >> null
        dagSubmitChecker.checkDAGInfoLengthByBusinessId(*_) >> {throw new StorageException(StorageErrorCode.DAG_LENGTH_LIMITATION.getCode(), "DAG length limitation")}
        dagDescriptorService.saveDescriptorVO(*_) >> descriptor_id
        when:
        facade.addDescriptor(null, "testBusiness", "testFeatureName", "release", "hello world")
        then:
        thrown TaskException
    }
}
