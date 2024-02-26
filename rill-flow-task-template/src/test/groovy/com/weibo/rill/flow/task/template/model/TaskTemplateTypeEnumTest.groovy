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
