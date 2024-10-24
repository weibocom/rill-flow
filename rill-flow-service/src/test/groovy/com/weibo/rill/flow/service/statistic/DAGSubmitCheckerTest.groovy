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

package com.weibo.rill.flow.service.statistic

import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.storage.exception.StorageException
import com.weibo.rill.flow.service.dconfs.BizDConfs
import spock.lang.Specification

class DAGSubmitCheckerTest extends Specification {
    SwitcherManager switcherManager = Mock(SwitcherManager)
    BizDConfs bizDConfs = Mock(BizDConfs)
    DAGSubmitChecker dagSubmitChecker = new DAGSubmitChecker(switcherManagerImpl: switcherManager, bizDConfs: bizDConfs)

    def "test checkDAGInfoLength when switcher is off"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> false
        expect:
        dagSubmitChecker.checkDAGInfoLength("testBusiness1:testFeatureName1_c_0dc48c1d-32a2", ["hello world".bytes])
    }

    def "test checkDAGInfoLength when switcher is on and be limited"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> true
        bizDConfs.getRedisBusinessIdToDAGInfoMaxLength() >> ["testBusiness1": 5]
        when:
        dagSubmitChecker.checkDAGInfoLength("testBusiness1:testFeatureName1_c_0dc48c1d-32a2", ["hello world".bytes])
        then:
        thrown StorageException
    }

    def "test checkDAGInfoLength when switcher is on"() {
        given:
        switcherManager.getSwitcherState("ENABLE_DAG_INFO_LENGTH_CHECK") >> true
        bizDConfs.getRedisBusinessIdToDAGInfoMaxLength() >> ["testBusiness1": 5]
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
