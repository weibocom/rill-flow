package com.weibo.rill.flow.service.dispatcher

import com.weibo.rill.flow.interfaces.model.http.HttpParameter
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Specification

class FunctionProtocolDispatcherTest extends Specification {
    FunctionProtocolDispatcher dispatcher = new FunctionProtocolDispatcher();

    def "buildHttpEntity test"() {
        given:
        def httpParameter = HttpParameter.builder()
                .header(inputHeader)
                .body(inputBody)
                .build()
        MultiValueMap<String, String> header = new LinkedMultiValueMap<>()
        Optional.ofNullable(httpParameter.getHeader())
                .ifPresent { it -> it.forEach { key, value -> header.add(key, value) } }

        when:
        def httpEntity = dispatcher.buildHttpEntity(method, header, httpParameter)

        then:
        httpEntity.body == body

        where:
        method          | inputHeader                                                   | inputBody                     | body
        null            | [:]                                                           | [:]                           | null
        HttpMethod.GET  | [:]                                                           | [:]                           | null
        HttpMethod.POST | [:]                                                           | [:]                           | [:]
        HttpMethod.POST | [:]                                                           | [k: "v", user: [name: "Bob"]] | [k: "v", user: [name: "Bob"]]
        HttpMethod.POST | ["Content-Type": MediaType.APPLICATION_JSON_VALUE]            | [k: "v", user: [name: "Bob"]] | [k: "v", user: [name: "Bob"]]
        HttpMethod.POST | ["Content-Type": MediaType.APPLICATION_FORM_URLENCODED_VALUE] | [k: "v", name: "Bob"]         | [k: ["v"], name: ["Bob"]]
    }

}
