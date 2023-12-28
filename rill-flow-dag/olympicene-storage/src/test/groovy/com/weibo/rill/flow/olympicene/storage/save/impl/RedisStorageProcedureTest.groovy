package com.weibo.rill.flow.olympicene.storage.save.impl

import com.weibo.rill.flow.olympicene.storage.redis.lock.Locker
import spock.lang.Specification

class RedisStorageProcedureTest extends Specification {
    Locker locker = Mock(Locker.class)
    RedisStorageProcedure redisStorageProcedure = new RedisStorageProcedure('instanceId', locker)

    def "lock and unlock should be invoked if no exception"() {
        when:
        redisStorageProcedure.lockAndRun("lockName", new Runnable() {
            @Override
            void run() {

            }
        })

        then:
        1 * locker.lock(*_)
        1 * locker.unlock(*_)
    }

    def "lock and unlock should be invoked if throw exception"() {
        when:
        redisStorageProcedure.lockAndRun("lockName", new Runnable() {
            @Override
            void run() {
                throw new Exception()
            }
        })

        then:
        thrown(Exception)
        1 * locker.lock(*_)
        1 * locker.unlock(*_)
    }
}
