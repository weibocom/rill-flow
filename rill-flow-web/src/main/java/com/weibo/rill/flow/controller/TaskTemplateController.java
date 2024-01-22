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

    @ApiOperation(value = "查询元数据")
    @RequestMapping(value = "get_meta_data_list.json", method = RequestMethod.GET)
    public JSONObject getMetaDataList(User flowUser) {
        JSONArray metaDataList = taskTemplateFacade.getTaskMetaDataList();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", metaDataList);
        return jsonObject;
    }
}
