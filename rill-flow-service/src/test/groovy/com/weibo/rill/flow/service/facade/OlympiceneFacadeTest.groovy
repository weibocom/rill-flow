package com.weibo.rill.flow.service.facade

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.common.exception.TaskException
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.traversal.Olympicene
import com.weibo.rill.flow.service.context.DAGContextInitializer
import com.weibo.rill.flow.service.dconfs.BizDConfs
import com.weibo.rill.flow.service.manager.DescriptorManager
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker
import com.weibo.rill.flow.service.statistic.ProfileRecordService
import spock.lang.Specification

class OlympiceneFacadeTest extends Specification {
    OlympiceneFacade facade = new OlympiceneFacade()
    ProfileRecordService profileRecordService = new ProfileRecordService()
    DAGSubmitChecker submitChecker = Mock(DAGSubmitChecker)
    DAGContextInitializer dagContextInitializer = new DAGContextInitializer()
    Olympicene olympicene = Mock(Olympicene)
    BizDConfs bizDConfs = Mock(BizDConfs)
    DAGStringParser dagStringParser = Mock(DAGStringParser)
    DescriptorManager descriptorManager = Mock(DescriptorManager)
    DAG dag = new DAG()

    def setup() {
        facade.profileRecordService = profileRecordService
        facade.dagSubmitChecker = submitChecker
        facade.dagContextInitializer = dagContextInitializer
        facade.olympicene = olympicene
        facade.dagStringParser = dagStringParser
        facade.descriptorManager = descriptorManager
        dagContextInitializer.bizDConfs = bizDConfs

        submitChecker.getCheckConfig(_) >> null
        submitChecker.check(*_) >> null
        descriptorManager.getDagDescriptor(*_) >> null
        dagStringParser.parse(_) >> dag
    }

    def "test submit"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 10240
        expect:
        facade.submit(1L, "testBusiness:testFeatureName", new JSONObject(["resourceName": "testCallbackUrl"]).toJSONString(), null, new JSONObject(["a": 1]), null)
    }

    def "test submit exception by limit max context size"() {
        given:
        bizDConfs.getRuntimeSubmitContextMaxSize() >> 0
        when:
        facade.submit(1L, "testBusiness:testFeatureName", new JSONObject(["resourceName": "testCallbackUrl"]).toJSONString(), null, new JSONObject(["a": 1]), null)
        then:
        thrown TaskException
    }
}
