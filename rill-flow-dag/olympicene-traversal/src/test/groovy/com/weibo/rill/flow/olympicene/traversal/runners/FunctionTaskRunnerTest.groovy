package com.weibo.rill.flow.olympicene.traversal.runners

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import spock.lang.Specification

class FunctionTaskRunnerTest extends Specification {
    FunctionTaskRunner runner = new FunctionTaskRunner(null, null, null, null, null, null)

    def "test base properties getter"() {
        expect:
        runner.getCategory() == TaskCategory.FUNCTION
        runner.getIcon() == ""
    }

    def "test getFields"() {
        when:
        JSONObject fields = runner.getFields()
        then:
        fields.size() == 12
        fields.getJSONObject("next").getString("name") == "下一节点"
        fields.getJSONObject("next").getString("type") == "string"
        fields.getJSONObject("next").getBoolean("required") == false
    }
}
