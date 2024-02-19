package com.weibo.rill.flow.service.facade

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.service.context.DAGContextInitializer
import com.weibo.rill.flow.service.dconfs.BizDConfs
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker
import com.weibo.rill.flow.service.statistic.ProfileRecordService
import spock.lang.Specification

class OlympiceneFacadeTest extends Specification {
    OlympiceneFacade facade = new OlympiceneFacade()
    ProfileRecordService profileRecordService = new ProfileRecordService()
    DAGSubmitChecker submitChecker = Mock(DAGSubmitChecker)
    DAGContextInitializer dagContextInitializer = new DAGContextInitializer()
    BizDConfs bizDConfs = Mock(BizDConfs)

    def setup() {
        facade.profileRecordService = profileRecordService
        facade.submitChecker = submitChecker
        facade.dagContextInitializer = dagContextInitializer
        submitChecker.getCheckConfig(_) >> null
        dagContextInitializer.bizDConfs = bizDConfs
    }

    def "test submit exception by limit max context size"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 0
        when:
        facade.submit(1L, "testBusiness:testFeatureName", "testCallbackUrl", null, new JSONObject(["a": 1]), null)
        then:
        thrown TaskException
    }
}
