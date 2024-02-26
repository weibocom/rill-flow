package com.weibo.rill.flow.olympicene.storage.redis.lock

import spock.lang.Specification

class ResourceLoaderTest extends Specification {
    def "test loadResourceAsText"() {
        expect:
        ResourceLoader.loadResourceAsText("test.json") == "{\"hello\": \"world\"}"
    }

    def "test loadResourceAsText with non-existing resource"() {
        when:
        ResourceLoader.loadResourceAsText("non-existing.txt")

        then:
        thrown(IOException)
    }

    def "test loadResourceAsText with null resource name"() {
        when:
        ResourceLoader.loadResourceAsText(null)

        then:
        thrown(NullPointerException)
    }
}
