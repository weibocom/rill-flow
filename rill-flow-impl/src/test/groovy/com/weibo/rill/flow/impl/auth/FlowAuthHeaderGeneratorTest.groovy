package com.weibo.rill.flow.impl.auth

import com.weibo.rill.flow.service.dconfs.BizDConfs
import org.springframework.http.HttpHeaders
import spock.lang.Specification

class FlowAuthHeaderGeneratorTest extends Specification {
    BizDConfs bizDConfs = Mock(BizDConfs)
    FlowAuthHeaderGenerator flowAuthHeaderGenerator = new FlowAuthHeaderGenerator(bizDConfs: bizDConfs, authSecret: 123,
            flowServerHost: "http://127.0.0.1", flowCallbackUri: "/flow/trigger.json")

    def setup() {
        bizDConfs.getGenerateAuthHeaderBusinessIds() >> ["business1"]
    }

    def "test appendRequestHeader when execution id is null"() {
        given:
        HttpHeaders httpHeaders = new HttpHeaders()
        when:
        flowAuthHeaderGenerator.appendRequestHeader(httpHeaders, null, null, Map.of())
        then:
        !httpHeaders.get("X-Callback-Url").get(0).contains("sign")
    }

    def "test appendRequestHeader when execution id not matches business ids"() {
        given:
        HttpHeaders httpHeaders = new HttpHeaders()
        Map<String, Object> input = new HashMap<>()
        when:
        flowAuthHeaderGenerator.appendRequestHeader(httpHeaders, "business2", null, input)
        then:
        !httpHeaders.get("X-Callback-Url").get(0).contains("sign")
    }

    def "test appendRequestHeader when execution id matches business ids"() {
        given:
        HttpHeaders httpHeaders = new HttpHeaders()
        Map<String, Object> input = new HashMap<>()
        when:
        flowAuthHeaderGenerator.appendRequestHeader(httpHeaders, "business1", null, input)
        then:
        httpHeaders.get("X-Callback-Url").get(0).contains("sign")
    }

    def "test appendRequestHeader when execution id not matches business ids but generate_auth is 1"() {
        given:
        HttpHeaders httpHeaders = new HttpHeaders()
        Map<String, Object> input = new HashMap<>()
        input.put("generate_auth", "1")
        when:
        flowAuthHeaderGenerator.appendRequestHeader(httpHeaders, "business2", null, input)
        then:
        httpHeaders.get("X-Callback-Url").get(0).contains("sign")
    }

    def "test appendRequestHeader when execution id not matches business ids but generate_auth is true"() {
        given:
        HttpHeaders httpHeaders = new HttpHeaders()
        Map<String, Object> input = new HashMap<>()
        input.put("generate_auth", true)
        when:
        flowAuthHeaderGenerator.appendRequestHeader(httpHeaders, "business2", null, input)
        then:
        httpHeaders.get("X-Callback-Url").get(0).contains("sign")
    }
}
