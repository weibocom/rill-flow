package com.weibo.rill.flow.task.template.dao.provider

import com.weibo.rill.flow.task.template.dao.model.TaskTemplateDO
import com.weibo.rill.flow.task.template.model.TaskTemplateParams
import spock.lang.Specification

class TaskTemplateProviderTest extends Specification {
    TaskTemplateProvider provider = new TaskTemplateProvider()

    def "test insert"() {
        given:
        TaskTemplateDO taskTemplateDO = new TaskTemplateDO()
        taskTemplateDO.setCategory("function")
        taskTemplateDO.setName("function template")
        taskTemplateDO.setId(1L)
        taskTemplateDO.setEnable(1)
        taskTemplateDO.setType(0)
        taskTemplateDO.setCreateTime(new Date())
        taskTemplateDO.setUpdateTime(new Date())

        when:
        String sql = provider.insert(taskTemplateDO)

        then:
        sql == "INSERT INTO task_template\n" +
                " (`name`, `type`, `category`, `icon`, `task_yaml`, `schema`, `output`, `enable`, `create_time`, `update_time`)\n" +
                "VALUES (#{name}, #{type}, #{category}, #{icon}, #{taskYaml}, #{schema}, #{output}, #{enable}, #{createTime}, #{updateTime})"
    }

    def "test update"() {
        given:
        TaskTemplateDO taskTemplateDO = new TaskTemplateDO()
        taskTemplateDO.setCategory("function")
        taskTemplateDO.setId(1L)
        taskTemplateDO.setType(0)
        when:
        String sql = provider.update(taskTemplateDO)
        then:
        sql == "UPDATE task_template\n" +
                "SET `type` = #{type}, `category` = #{category}\n" +
                "WHERE (id = #{id})"
    }

    def "test update when id is null"() {
        given:
        TaskTemplateDO taskTemplateDO = new TaskTemplateDO()
        taskTemplateDO.setCategory("function")
        taskTemplateDO.setType(0)
        when:
        provider.update(taskTemplateDO)
        then:
        thrown RuntimeException
    }

    def "test getTaskTemplateList"() {
        given:
        TaskTemplateParams taskTemplateParams = new TaskTemplateParams()
        taskTemplateParams.setCategory("function")
        taskTemplateParams.setType(0)
        taskTemplateParams.setId(1L)
        taskTemplateParams.setEnable(1)
        taskTemplateParams.setName("func")
        taskTemplateParams.setOffset(0)
        taskTemplateParams.setLimit(10)
        when:
        String sql = provider.getTaskTemplateList(taskTemplateParams)
        then:
        sql == "SELECT `id`,`name`,`type`,`category`,`icon`,`task_yaml`,`schema`,`output`,`enable`,`create_time`,`update_time`\n" +
                "FROM task_template\n" +
                "WHERE (`id` = #{id} AND `type` = #{type} AND `name` like '%' #{name} '%' AND `category` = #{category} AND `enable` = #{enable})\n" +
                "ORDER BY `id` asc LIMIT 10 OFFSET 0"
    }

    def "test getTaskTemplateList by default"() {
        given:
        TaskTemplateParams taskTemplateParams = new TaskTemplateParams()
        when:
        String sql = provider.getTaskTemplateList(taskTemplateParams)
        then:
        sql == "SELECT `id`,`name`,`type`,`category`,`icon`,`task_yaml`,`schema`,`output`,`enable`,`create_time`,`update_time`\n" +
                "FROM task_template\n" +
                "ORDER BY `id` asc LIMIT 0 OFFSET 0"
    }

    def "test disable"() {
        when:
        String sql = provider.disable(1)
        then:
        sql == "UPDATE task_template\n" +
                "SET `enable` = 0\n" +
                "WHERE (`id` = #{id})"
    }

    def "test enable"() {
        when:
        String sql = provider.enable(1)
        then:
        sql == "UPDATE task_template\n" +
                "SET `enable` = 1\n" +
                "WHERE (`id` = #{id})"
    }
}
