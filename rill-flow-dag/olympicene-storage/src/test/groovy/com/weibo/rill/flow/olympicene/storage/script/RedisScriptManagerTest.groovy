package com.weibo.rill.flow.olympicene.storage.script

import spock.lang.Specification

class RedisScriptManagerTest extends Specification {
    def "no exception when load script"() {
        when:
        RedisScriptManager.getRedisGet()
        then:
        noExceptionThrown()
    }
}
