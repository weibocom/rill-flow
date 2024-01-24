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

    public List<TaskTemplate> getTaskTemplates(int page, int pageSize) {
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
            if (!taskRunner.isEnable()) {
                continue;
            }
            metaDataList.add(taskRunner);
        }
        if (preSize < metaDataList.size()) {
            for (int i = preSize; i < metaDataList.size(); i++) {
                taskTemplateList.add(turnMetaDataToTaskTemplate(metaDataList.get(i)));
            }
        }
        // TODO: 查询数据库，填充列表
        return taskTemplateList;
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
        result.setNodeType("meta");
        result.setMetaData(MetaData.builder().icon(taskRunner.getIcon()).fields(taskRunner.getFields()).build());
        return result;
    }
}
