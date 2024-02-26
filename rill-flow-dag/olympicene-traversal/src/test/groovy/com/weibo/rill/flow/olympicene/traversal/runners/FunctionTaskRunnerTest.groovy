/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
