package com.weibo.rill.flow.service.statistic

import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.storage.exception.StorageException
import com.weibo.rill.flow.service.dconfs.BizDConfs
import spock.lang.Specification

class DAGSubmitCheckerTest extends Specification {
    SwitcherManager switcherManager = Mock(SwitcherManager)
    BizDConfs bizDConfs = Mock(BizDConfs)
    DAGSubmitChecker dagSubmitChecker = new DAGSubmitChecker(switcherManagerImpl: switcherManager, bizDConfs: bizDConfs)

    def setup() {
        bizDConfs.getRedisBusinessIdToDAGInfoMaxLength() >> ["testBusiness1": 5]
    }

    def "test checkDAGInfoLength when switcher is off"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> false
        expect:
        dagSubmitChecker.checkDAGInfoLength("testBusiness1:testFeatureName1_c_0dc48c1d-32a2", ["hello world".bytes])
    }

    def "test checkDAGInfoLength when switcher is on and be limited"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> true
        when:
        dagSubmitChecker.checkDAGInfoLength("testBusiness1:testFeatureName1_c_0dc48c1d-32a2", ["hello world".bytes])
        then:
        thrown StorageException
    }

    def "test checkDAGInfoLength when switcher is on"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> true
        expect:
        dagSubmitChecker.checkDAGInfoLength("testBusiness1:testFeatureName1_c_0dc48c1d-32a2", null)
        dagSubmitChecker.checkDAGInfoLength("testBusiness2:testFeatureName1_c_0dc48c1d-32a2", ["hello world".bytes])
    }

    def "test checkDAGInfoLength when switcher is on and dconfs return null"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> true
        bizDConfs.getRedisBusinessIdToDAGInfoMaxLength() >> null
        expect:
        dagSubmitChecker.checkDAGInfoLength("testBusiness2:testFeatureName1_c_0dc48c1d-32a2", ["hello world".bytes])
    }
}
