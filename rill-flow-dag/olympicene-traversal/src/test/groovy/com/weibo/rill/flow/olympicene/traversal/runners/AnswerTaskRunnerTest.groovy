package com.weibo.rill.flow.olympicene.traversal.runners

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.interfaces.model.task.InvokeTimeInfo
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import spock.lang.Specification

class AnswerTaskRunnerTest extends Specification {
    DAGInfoStorage dagInfoStorage = Mock(DAGInfoStorage)
    DAGDispatcher answerTaskDispatcher = Mock(DAGDispatcher)
    AnswerTaskRunner runner = new AnswerTaskRunner(answerTaskDispatcher, null, dagInfoStorage, null, null, null)

    def setup() {
        dagInfoStorage.saveTaskInfos(*_) >> null
    }

    def "test base properties getter"() {
        expect:
        runner.getCategory() == TaskCategory.ANSWER
        runner.getIcon() == "ant-design:audio-outlined"
    }

    def "test getFields"() {
        when:
        JSONObject fields = runner.getFields()
        then:
        fields.size() == 6
        fields.getJSONObject("next").getString("title") == "下一节点"
        fields.getJSONObject("next").getString("type") == "string"
        fields.getJSONObject("next").getBoolean("required") == false
    }

    def "test doRun when answerTask is null"() {
        given:
        TaskInfo taskInfo = new TaskInfo()
        TaskInvokeMsg taskInvokeMsg = Mock(TaskInvokeMsg)
        InvokeTimeInfo invokeTimeInfo = new InvokeTimeInfo()
        taskInvokeMsg.getInvokeTimeInfos() >> [invokeTimeInfo]
        taskInfo.getTaskInvokeMsg() >> taskInvokeMsg
        when:
        def result = runner.doRun("test-execution-id", taskInfo, [:])
        then:
        result.taskStatus == TaskStatus.SKIPPED
    }
}
