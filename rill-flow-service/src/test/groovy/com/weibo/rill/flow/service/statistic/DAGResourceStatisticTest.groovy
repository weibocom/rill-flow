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
