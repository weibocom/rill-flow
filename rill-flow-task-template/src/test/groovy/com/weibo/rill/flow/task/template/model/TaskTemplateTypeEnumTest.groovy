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

package com.weibo.rill.flow.task.template.model

import spock.lang.Specification

class TaskTemplateTypeEnumTest extends Specification {
    def "test TaskTemplateTypeEnum"() {
        when:
        TaskTemplateTypeEnum taskTemplateTypeEnum = TaskTemplateTypeEnum.getEnumByType(type)
        then:
        taskTemplateTypeEnum == expected
        where:
        type    | expected
        null    | null
        0       | TaskTemplateTypeEnum.FUNCTION
        1       | TaskTemplateTypeEnum.PLUGIN
        2       | TaskTemplateTypeEnum.LOGIC
        3       | null
    }

    def "test getter"() {
        expect:
        TaskTemplateTypeEnum.FUNCTION.getType() == 0
        TaskTemplateTypeEnum.PLUGIN.getType() == 1
        TaskTemplateTypeEnum.LOGIC.getType() == 2
    }
}
