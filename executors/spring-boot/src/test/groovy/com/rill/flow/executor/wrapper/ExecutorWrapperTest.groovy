package com.rill.flow.executor.wrapper

import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.function.Function

class ExecutorWrapperTest extends Specification {
    ExecutorWrapper executorWrapper
    ExecutorService executorService = Mock()
    Function<ExecutorContext, Map<String, Object>> emptyFunction = Mock()

    def setup() {
        executorWrapper = new ExecutorWrapper(executorService)
    }

    def "execute should async submit job when mode is 'async'"() {
        when:
        executorWrapper.execute(new ExecutorContext('', 'async', [:]), emptyFunction)

        then:
        1 * executorService.submit(_)
    }

    def "executor should sync submit job when mode is not 'async'"() {
        when:
        executorWrapper.execute(new ExecutorContext('', mode, [:]), emptyFunction)

        then:
        0 * executorService.submit(_)
        1 * emptyFunction.apply(_)

        where:
        mode << ['sync','',null]
    }

}
