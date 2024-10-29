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
