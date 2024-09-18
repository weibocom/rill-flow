package com.weibo.rill.flow.olympicene.traversal.runners

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import spock.lang.Specification

class AnswerTaskRunnerTest extends Specification {
    AnswerTaskRunner runner = new AnswerTaskRunner(null, null, null, null, null, null)

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
}
