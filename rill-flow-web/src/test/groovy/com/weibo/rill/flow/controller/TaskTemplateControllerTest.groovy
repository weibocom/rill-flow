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

package com.weibo.rill.flow.controller

import com.alibaba.fastjson.JSONObject
import com.weibo.rill.flow.task.template.model.TaskTemplate
import com.weibo.rill.flow.task.template.model.TaskTemplateParams
import com.weibo.rill.flow.task.template.service.TaskTemplateService
import spock.lang.Specification

class TaskTemplateControllerTest extends Specification {
    TaskTemplateController taskTemplateController = new TaskTemplateController()
    TaskTemplateService taskTemplateService = Mock(TaskTemplateService)
    def setup() {
        taskTemplateController.taskTemplateService = taskTemplateService
    }

    def "test getMetaDataList"() {
        given:
        taskTemplateService.getTaskMetaDataList() >> [["name": "function"], ["name": "pass"]]
        when:
        def result = taskTemplateController.getMetaDataList(null)
        then:
        result == ["data": [["name": "function"], ["name": "pass"]]]
    }

    def "test getTaskTemplates with valid params"() {
        given:
        var taskTemplate = new TaskTemplate(id: 1L, name: "test", category: "function", type: 1, nodeType: "spark", enable: 1)
        TaskTemplateParams params = TaskTemplateParams.builder().id(1L).name("test").category("function").type(1).nodeType("spark").enable(1).build()
        taskTemplateService.getTaskTemplates(params, 1, 10) >> [taskTemplate]
        when:
        def result = taskTemplateController.getTaskTemplates(null, 1, 10, 1L, "test", "function", 1, "spark", 1)
        then:
        result == ["data": [taskTemplate]]
    }

    def "test getTaskTemplates with invalid params"() {
        given:
        TaskTemplateParams params = TaskTemplateParams.builder().id(-1L).name("").category("").type(-1).nodeType("").enable(-1).build()
        taskTemplateService.getTaskTemplates(params, 1, 10) >> []
        when:
        def result = taskTemplateController.getTaskTemplates(null, 1, 10, -1L, "", "", -1, "", -1)
        then:
        result == ["data": []]
    }

    def "test disableTaskTemplate with valid id"() {
        given:
        taskTemplateService.disableTaskTemplate(1L) >> 1
        when:
        def result = taskTemplateController.deleteTaskTemplate(null, 1L)
        then:
        result == ["code": 0]
    }

    def "test disableTaskTemplate with invalid id"() {
        given:
        taskTemplateService.disableTaskTemplate(-1L) >> 0
        when:
        def result = taskTemplateController.deleteTaskTemplate(null, -1L)
        then:
        result == ["code": 1]
    }

    def "test enableTaskTemplateFacade with valid id"() {
        given:
        taskTemplateService.enableTaskTemplate(1L) >> 1
        when:
        def result = taskTemplateController.enableTaskTemplateFacade(null, 1L)
        then:
        result == ["code": 0]
    }

    def "test enableTaskTemplateFacade with invalid id"() {
        given:
        taskTemplateService.enableTaskTemplate(-1L) >> 0
        when:
        def result = taskTemplateController.enableTaskTemplateFacade(null, -1L)
        then:
        result == ["code": 1]
    }

    def "test createTaskTemplate with valid taskTemplate"() {
        given:
        JSONObject taskTemplate = new JSONObject(["name": "test", "category": "function", "type": 1, "nodeType": "spark", "enable": 1])
        taskTemplateService.createTaskTemplate(taskTemplate) >> 1L
        when:
        def result = taskTemplateController.createTaskTemplate(null, taskTemplate)
        then:
        result == ["code": 0, "data": ["id": 1L]]
    }

    def "test updateTaskTemplate with valid taskTemplate"() {
        given:
        JSONObject taskTemplate = new JSONObject(["id": 1L, "name": "test", "category": "function", "type": 1, "nodeType": "spark", "enable": 1])
        taskTemplateService.updateTaskTemplate(taskTemplate) >> 1
        when:
        def result = taskTemplateController.updateTaskTemplate(null, taskTemplate)
        then:
        result == ["code": 0]
    }

    def "test updateTaskTemplate with invalid taskTemplate"() {
        given:
        JSONObject taskTemplate = new JSONObject(["id": -1L, "name": "", "category": "", "type": -1, "nodeType": "", "enable": -1])
        taskTemplateService.updateTaskTemplate(taskTemplate) >> 0
        when:
        def result = taskTemplateController.updateTaskTemplate(null, taskTemplate)
        then:
        result == ["code": 1]
    }
}
