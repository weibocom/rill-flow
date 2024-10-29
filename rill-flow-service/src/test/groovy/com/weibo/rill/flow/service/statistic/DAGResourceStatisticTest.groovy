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

import com.alibaba.fastjson.JSONObject
import spock.lang.Specification
import spock.lang.Unroll

class DAGResourceStatisticTest extends Specification {
    DAGResourceStatistic statistic = new DAGResourceStatistic()

    @Unroll
    def "test getRetryIntervalSeconds"() {
        when:
        JSONObject urlRet1 = new JSONObject(Map.of("data", "message"))
        JSONObject urlRet2 = new JSONObject(Map.of("data", Map.of("sys_info", Map.of("retry_interval_seconds", 100))))
        JSONObject urlRet3 = new JSONObject(Map.of("error_detail", Map.of("retry_interval_seconds", 100)))
        then:
        statistic.getRetryIntervalSeconds(urlRet1) == 0
        statistic.getRetryIntervalSeconds(urlRet2) == 100
        statistic.getRetryIntervalSeconds(urlRet3) == 100
    }
}
