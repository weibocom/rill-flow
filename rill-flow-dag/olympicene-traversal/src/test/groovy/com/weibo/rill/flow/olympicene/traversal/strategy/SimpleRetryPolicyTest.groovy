package com.weibo.rill.flow.olympicene.traversal.strategy

import com.weibo.rill.flow.interfaces.model.strategy.Retry
import com.weibo.rill.flow.interfaces.model.task.InvokeTimeInfo
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.model.strategy.RetryContext
import spock.lang.Specification
import spock.lang.Unroll

class SimpleRetryPolicyTest extends Specification {
    SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy()
    RetryContext context1 = new RetryContext()
    RetryContext context2 = new RetryContext()
    RetryContext context3 = new RetryContext()
    RetryContext context4 = new RetryContext()

    def setup() {
        TaskInfo taskInfo = new TaskInfo()
        TaskInvokeMsg taskInvokeMsg = new TaskInvokeMsg()
        taskInvokeMsg.setInvokeTimeInfos(List.of(new InvokeTimeInfo(), new InvokeTimeInfo()))
        taskInfo.setTaskInvokeMsg(taskInvokeMsg)

        Retry retry1 = new Retry(3, 3, 2, null)
        context1.setRetryConfig(retry1)
        context1.setTaskInfo(taskInfo)
        context1.setTaskStatus(TaskStatus.FAILED)

        Retry retry2 = new Retry()
        retry2.setIntervalInSeconds(1)
        retry2.setMaxRetryTimes(3)
        context2.setRetryConfig(retry2)
        context2.setTaskInfo(taskInfo)
        context2.setTaskStatus(TaskStatus.SUCCEED)

        Retry retry3 = new Retry()
        retry3.setMaxRetryTimes(3)
        retry3.setConditions(List.of("\$.output.[?(@.a == 1)]", "\$.output.[?(@.b == 2)]"))
        context3.setRetryConfig(retry3)
        context3.setTaskInfo(taskInfo)
        context3.setTaskStatus(TaskStatus.FAILED)

        Retry retry4 = new Retry()
        retry4.setMaxRetryTimes(1)
        context4.setRetryConfig(retry4)
        context4.setTaskInfo(taskInfo)
        context4.setTaskStatus(TaskStatus.FAILED)
    }

    def "NeedRetry"() {
        expect:
        // need retry when failed
        simpleRetryPolicy.needRetry(context1) == true
        // do not need retry when succeed
        simpleRetryPolicy.needRetry(context2) == false
        // need retry when failed even with retry conditions but without output parameter
        simpleRetryPolicy.needRetry(context3) == true
        // do not need retry when failed with retried times equal or more than max retry times
        simpleRetryPolicy.needRetry(context4) == false
    }

    def "NeedRetryWithConditions"() {
        when:
        Map<String, Object> output1 = Map.of("a", 1, "b", 2, "c", 3)
        Map<String, Object> output2 = Map.of("a", 0, "b", 0, "c", 0)
        Map<String, Object> output3 = Map.of("a", 1, "b", 0, "c", 0)
        then:
        // need retry when failed and all retry conditions matched
        simpleRetryPolicy.needRetry(context1, output1) == true
        // do not need retry when succeed
        simpleRetryPolicy.needRetry(context2, output1) == false
        // need retry when failed and all retry conditions matched
        simpleRetryPolicy.needRetry(context3, output1) == true
        // do not need retry when failed and all retry conditions not matched
        simpleRetryPolicy.needRetry(context3, output2) == false
        // need retry when failed and any retry conditions matched
        simpleRetryPolicy.needRetry(context3, output3) == true
        // do not need retry when failed with retried times equal or more than max retry times
        simpleRetryPolicy.needRetry(context4, output1) == false
    }

    @Unroll
    def "CalculateRetryInterval"() {
        expect:
        // 3 * 2 ^ 2 == 12
        simpleRetryPolicy.calculateRetryInterval(context1) == 12
        // 1 * 1 ^ 2 == 1
        simpleRetryPolicy.calculateRetryInterval(context2) == 1
        // 0 * 1 ^ 2 == 0
        simpleRetryPolicy.calculateRetryInterval(context3) == 0
    }
}
