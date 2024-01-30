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

package com.weibo.rill.flow.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.olympicene.storage.dao.model.TaskTemplateParams;
import com.weibo.rill.flow.service.facade.TaskTemplateFacade;
import com.weibo.rill.flow.service.model.TaskTemplate;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@Api(tags = {"任务模板相关接口"})
@RequestMapping("/template")
public class TaskTemplateController {
    @Autowired
    private TaskTemplateFacade taskTemplateFacade;

    @ApiOperation(value = "查询元数据列表")
    @RequestMapping(value = "get_meta_data_list.json", method = RequestMethod.GET)
    public JSONObject getMetaDataList(User flowUser) {
        JSONArray metaDataList = taskTemplateFacade.getTaskMetaDataList();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", metaDataList);
        return jsonObject;
    }

    @ApiOperation(value = "查询任务模板列表")
    @RequestMapping(value = "get_task_templates.json", method = RequestMethod.GET)
    public JSONObject getTaskTemplates(User flowUser,
                                       @ApiParam(value = "页码") @RequestParam(value = "page", required = false, defaultValue = "1") int page,
                                       @ApiParam(value = "每页元素数") @RequestParam(value = "page_size", required = false, defaultValue = "10") int pageSize,
                                       @ApiParam(value = "模板id") @RequestParam(value = "id", required = false) Long id,
                                       @ApiParam(value = "模板名称") @RequestParam(value = "name", required = false) String name,
                                       @ApiParam(value = "元数据类别") @RequestParam(value = "category", required = false) String category,
                                       @ApiParam(value = "模板类型") @RequestParam(value = "type", required = false) Integer type) {
        TaskTemplateParams params = TaskTemplateParams.builder().id(id).name(name).category(category).type(type).build();
        List<TaskTemplate> taskTemplatePageInfo = taskTemplateFacade.getTaskTemplates(params, page, pageSize);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", taskTemplatePageInfo);
        return jsonObject;
    }

    @ApiOperation("插入模板接口")
    @RequestMapping(value = "create_task_template.json", method = RequestMethod.POST)
    public JSONObject createTaskTemplate(User user, @ApiParam(value = "任务模板对象") @RequestParam(value = "task_template") String taskTemplate) {
        long id = taskTemplateFacade.createTaskTemplate(taskTemplate);
        return new JSONObject(Map.of("code", 0, "data", Map.of("id", id)));
    }

    @ApiOperation("更新模板接口")
    @RequestMapping(value = "update_task_template.json", method = RequestMethod.POST)
    public JSONObject updateTaskTemplate(User user, @ApiParam(value = "任务模板对象") @RequestParam(value = "task_template") String taskTemplate) {
        int num = taskTemplateFacade.updateTaskTemplate(taskTemplate);
        return new JSONObject(Map.of("code", num > 0? 0: 1));
    }
}
