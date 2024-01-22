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
import com.weibo.rill.flow.service.facade.TaskTemplateFacade;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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
}
