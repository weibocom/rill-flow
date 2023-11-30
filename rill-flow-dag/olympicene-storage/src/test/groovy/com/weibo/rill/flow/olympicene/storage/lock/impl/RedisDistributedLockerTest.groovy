package com.weibo.rill.flow.olympicene.storage.lock.impl

import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.olympicene.storage.redis.lock.impl.RedisDistributedLocker
import spock.lang.Specification

class RedisDistributedLockerTest extends Specification {
    RedisClient redisClient = Mock(RedisClient.class)
    RedisDistributedLocker redisDistributedLocker = new RedisDistributedLocker(redisClient)

    def setup() {
        redisDistributedLocker.setLockTimeout(1000)
    }

    def "lock should return if redis return ok"() {
        when:
        redisDistributedLocker.lock("lockName", "instanceId", 500)

        then:
        loopTimes * redisClient.eval(*_) >>> redisRet
        noExceptionThrown()

        where:
        redisRet                                                | loopTimes
        ["OK".getBytes()]                                       | 1
        ["FAIL".getBytes(), "OK".getBytes(), "OK".getBytes()]   | 2
        ["FAIL".getBytes(), "FAIL".getBytes(), "OK".getBytes()] | 3
    }

    def "lock should throw exception if lock timeout"() {
        when:
        redisDistributedLocker.lock("lockName", "instanceId", 500)

        then:
        (_ .. 30) * redisClient.eval(*_) >> "FAIL".getBytes()
        thrown(RuntimeException)
    }
}
