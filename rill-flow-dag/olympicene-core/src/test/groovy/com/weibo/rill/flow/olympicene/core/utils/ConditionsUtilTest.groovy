package com.weibo.rill.flow.olympicene.core.utils


import spock.lang.Specification

class ConditionsUtilTest extends Specification {

    def "test conditionsAllMatch"() {
        given:
        Map<String, Object> input = Map.of("a", 1, "b", 2, "c", 3)
        when:
            boolean result = ConditionsUtil.conditionsAllMatch(conditions, input, "input")
        then:
            expected == result
        where:
        conditions  | expected
        List.of("\$.input.[?(@.a == 1)]", "\$.input.[?(@.b == 2)]", "\$.input.[?(@.c == 3)]") | true
        List.of("\$.input.[?(@.a == 1)]", "\$.input.[?(@.b == 1)]", "\$.input.[?(@.c == 3)]") | false
        List.of("\$.input.[?(@.a == 0)]", "\$.input.[?(@.b == 0)]", "\$.input.[?(@.c == 0)]") | false
    }

    def "test conditionsAnyMatch"() {
        given:
        Map<String, Object> input = Map.of("a", 1, "b", 2, "c", 3)
        when:
        boolean result = ConditionsUtil.conditionsAnyMatch(conditions, input, "input")
        then:
        expected == result
        where:
        conditions  | expected
        List.of("\$.input.[?(@.a == 1)]", "\$.input.[?(@.b == 2)]", "\$.input.[?(@.c == 3)]") | true
        List.of("\$.input.[?(@.a == 1)]", "\$.input.[?(@.b == 1)]", "\$.input.[?(@.c == 3)]") | true
        List.of("\$.input.[?(@.a == 0)]", "\$.input.[?(@.b == 0)]", "\$.input.[?(@.c == 0)]") | false
    }
}
