package com.weibo.rill.flow.olympicene.ddl

import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLMapper
import spock.lang.Specification

class YAMLMapperTest extends Specification {

    static class A {
        String a
        String b
        List<B> c

        A() {}

        String getA() {
            return a
        }

        String getB() {
            return b
        }

        List<B> getC() {
            return c
        }
    }

    static class B {
        String d
        String e

        B() {
        }
    }

    def "test mapper"() {
        given:
        String text = "a: ttt\n" +
                "b: tt\n" +
                "c:\n" +
                "  - d: 1\n" +
                "    e: 2\n" +
                "  - d: 2\n" +
                "    e: 3"
        when:
        A ret = YAMLMapper.parseObject(text, A.class)

        then:
        ret instanceof A
        ret.a == "ttt"
        ret.b == "tt"
        ret.c.size() == 2
        ret.c.get(0).d == "1"
        ret.c.get(0).e == "2"
        ret.c.get(1).d == "2"
        ret.c.get(1).e == "3"
    }

}
