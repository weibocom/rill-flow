package com.weibo.rill.flow.olympicene.traversal.utils

import spock.lang.Specification
import java.util.concurrent.atomic.AtomicInteger

class OperationUtilTest extends Specification {
    def "test OPERATE_WITH_RETRY"() {
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
}
