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

package com.weibo.rill.flow.service.facade;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.storage.dao.mapper.TaskTemplateDAO;
import com.weibo.rill.flow.olympicene.storage.dao.model.TaskTemplateDO;
import com.weibo.rill.flow.olympicene.storage.dao.model.TaskTemplateParams;
import com.weibo.rill.flow.olympicene.storage.dao.model.TaskTemplateTypeEnum;
import com.weibo.rill.flow.olympicene.traversal.runners.AbstractTaskRunner;
import com.weibo.rill.flow.service.model.MetaData;
import com.weibo.rill.flow.service.model.TaskTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class TaskTemplateFacade {
    @Autowired
    private Map<String, AbstractTaskRunner> taskRunnerMap;
    @Autowired
    private TaskTemplateDAO taskTemplateDAO;

    public JSONArray getTaskMetaDataList() {
        JSONArray metaDataList = new JSONArray();
        for (Map.Entry<String, AbstractTaskRunner> taskRunnerEntry: taskRunnerMap.entrySet()) {
            JSONObject metaData = new JSONObject();
            AbstractTaskRunner taskRunner = taskRunnerEntry.getValue();
            if (!taskRunner.isEnable()) {
                continue;
            }
            metaData.put("category", taskRunner.getCategory());
            metaData.put("icon", taskRunner.getIcon());
            metaData.put("fields", taskRunner.getFields());
            metaDataList.add(metaData);
        }
        return metaDataList;
    }

    public List<TaskTemplate> getTaskTemplates(TaskTemplateParams params, int page, int pageSize) {
        if (page <= 0) {
            page = 1;
        }
        if (pageSize < 10 || pageSize > 50) {
            pageSize = 10;
        }

        int preSize = pageSize * (page - 1);
        List<TaskTemplate> taskTemplateList = new ArrayList<>();
        List<AbstractTaskRunner> metaDataList = new ArrayList<>();
        for (Map.Entry<String, AbstractTaskRunner> taskRunnerEntry: taskRunnerMap.entrySet()) {
            AbstractTaskRunner taskRunner = taskRunnerEntry.getValue();
            if (!taskRunner.isEnable() || params.getId() != null
                    || params.getName() != null && !taskRunner.getCategory().contains(params.getName())
                    || params.getCategory() != null && !taskRunner.getCategory().equals(params.getCategory())
                    || params.getType() != null && params.getType() == 1
                    || params.getType() != null && params.getType() == 0 && !"function".equals(taskRunner.getCategory())
                    || params.getType() != null && params.getType() == 2 && "function".equals(taskRunner.getCategory())
            ) {
                continue;
            }
            metaDataList.add(taskRunner);
        }
        if (preSize <= metaDataList.size()) {
            for (int i = preSize; i < metaDataList.size() && i < pageSize; i++) {
                taskTemplateList.add(turnMetaDataToTaskTemplate(metaDataList.get(i)));
            }
            preSize = 0;
            pageSize -= taskTemplateList.size();
        } else {
            preSize -= metaDataList.size();
        }
        if (pageSize <= 0) {
            return taskTemplateList;
        }
        // TODO: 查询数据库，填充列表
        params.setOffset(preSize);
        params.setLimit(pageSize);
        List<TaskTemplateDO> taskTemplateDOList = taskTemplateDAO.getTaskTemplateList(params);
        if (taskTemplateDOList != null) {
            for (TaskTemplateDO taskTemplateDO : taskTemplateDOList) {
                taskTemplateList.add(turnTaskTemplateDOToTaskTemplate(taskTemplateDO));
            }
        }
        return taskTemplateList;
    }

    private TaskTemplate turnTaskTemplateDOToTaskTemplate(TaskTemplateDO taskTemplateDO) {
        TaskTemplate result = new TaskTemplate();
        result.setId(taskTemplateDO.getId());
        result.setCategory(taskTemplateDO.getCategory());
        result.setIcon(taskTemplateDO.getIcon());
        result.setTaskYaml(taskTemplateDO.getTaskYaml());
        result.setName(taskTemplateDO.getName());
        result.setOutput(taskTemplateDO.getOutput());
        result.setSchema(taskTemplateDO.getSchema());
        result.setType(taskTemplateDO.getType());
        result.setTypeStr(TaskTemplateTypeEnum.getEnumByType(taskTemplateDO.getType()).getDesc());
        result.setNodeType("template");
        AbstractTaskRunner taskRunner = taskRunnerMap.get(taskTemplateDO.getCategory());
        MetaData metaData = MetaData.builder().icon(taskRunner.getIcon()).fields(taskRunner.getFields()).build();
        result.setMetaData(metaData);
        return result;
    }

    private TaskTemplate turnMetaDataToTaskTemplate(AbstractTaskRunner taskRunner) {
        TaskTemplate result = new TaskTemplate();
        result.setId(null);
        result.setCategory(taskRunner.getCategory());
        result.setIcon(taskRunner.getIcon());
        result.setTaskYaml("");
        result.setName(taskRunner.getCategory());
        result.setOutput("{}");
        result.setSchema("{}");
        result.setType("function".equals(taskRunner.getCategory())? 0: 2);
        result.setTypeStr(result.getType() == 0? "函数节点": "逻辑节点");
        result.setNodeType("meta");
        result.setMetaData(MetaData.builder().icon(taskRunner.getIcon()).fields(taskRunner.getFields()).build());
        return result;
    }
}
