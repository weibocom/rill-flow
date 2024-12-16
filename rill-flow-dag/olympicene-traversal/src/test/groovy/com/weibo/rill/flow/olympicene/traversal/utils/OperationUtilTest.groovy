package com.weibo.rill.flow.olympicene.traversal.utils

import spock.lang.Specification
import java.util.concurrent.atomic.AtomicInteger

class OperationUtilTest extends Specification {
    def "test OPERATE_WITH_RETRY with retries"() {
        given:
        AtomicInteger counter = new AtomicInteger(0)
        Runnable operation = { 
            if (counter.incrementAndGet() < 3) {
                throw new RuntimeException("Operation failed")
            }
        }

        when:
        OperationUtil.OPERATE_WITH_RETRY.accept(operation, 3)

        then:
        counter.get() == 3
    }

    def "test OPERATE_WITH_RETRY with zero retries"() {
        given:
        AtomicInteger counter = new AtomicInteger(0)
        Runnable operation = { 
            counter.incrementAndGet()
        }

        when:
        OperationUtil.OPERATE_WITH_RETRY.accept(operation, 0)

        then:
        counter.get() == 1
    }
}
