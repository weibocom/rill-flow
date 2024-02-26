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

package com.weibo.rill.flow.impl.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.task.template.dao.mapper.TaskTemplateDAO;
import com.weibo.rill.flow.task.template.dao.model.TaskTemplateDO;
import com.weibo.rill.flow.task.template.model.TaskTemplateParams;
import com.weibo.rill.flow.task.template.model.TaskTemplateTypeEnum;
import com.weibo.rill.flow.olympicene.traversal.runners.AbstractTaskRunner;
import com.weibo.rill.flow.task.template.model.MetaData;
import com.weibo.rill.flow.task.template.model.TaskTemplate;
import com.weibo.rill.flow.task.template.service.TaskTemplateService;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class TaskTemplateServiceImpl implements TaskTemplateService {
    @Autowired
    private Map<String, AbstractTaskRunner> taskRunnerMap;
    @Autowired
    private TaskTemplateDAO taskTemplateDAO;

    private final static int minPage = 1;
    private final static int minPageSize = 10;
    private final static int maxPageSize = 50;

    public JSONArray getTaskMetaDataList() {
        JSONArray metaDataList = new JSONArray();
        for (Map.Entry<String, AbstractTaskRunner> taskRunnerEntry: taskRunnerMap.entrySet()) {
            JSONObject metaData = new JSONObject();
            AbstractTaskRunner taskRunner = taskRunnerEntry.getValue();
            if (!taskRunner.isEnable()) {
                continue;
            }
            metaData.put("category", taskRunner.getCategory().getValue());
            metaData.put("icon", taskRunner.getIcon());
            metaData.put("fields", taskRunner.getFields());
            metaDataList.add(metaData);
        }
        return metaDataList;
    }

    public List<TaskTemplate> getTaskTemplates(TaskTemplateParams params, int page, int pageSize) {
        if (page < minPage) {
            page = minPage;
        }
        if (pageSize < minPageSize || pageSize > maxPageSize) {
            pageSize = minPageSize;
        }

        // 已展示元素数量
        int preSize = pageSize * (page - 1);
        List<TaskTemplate> taskTemplateList = new ArrayList<>();
        List<AbstractTaskRunner> metaDataList = new ArrayList<>();
        if ((params.getNodeType() == null || params.getNodeType().equals("meta")) && (params.getEnable() == null || params.getEnable() == 1)) {
            metaDataList = getTaskRunners(params);
        }

        // 已展示元素数量小于元数据列表数量，说明需要用元数据填充列表
        if (preSize < metaDataList.size()) {
            for (; preSize < metaDataList.size() && taskTemplateList.size() < pageSize; preSize++) {
                taskTemplateList.add(turnMetaDataToTaskTemplate(metaDataList.get(preSize)));
            }
            pageSize -= taskTemplateList.size();
        }

        // 将 preSize 转化为数据库偏移量
        preSize -= metaDataList.size();
        if (pageSize <= 0) {
            return taskTemplateList;
        }

        // 查询数据库，填充列表
        List<TaskTemplate> taskTemplatesFromDB = getTaskTemplatesFromDB(params, pageSize, preSize);
        taskTemplateList.addAll(taskTemplatesFromDB);

        return taskTemplateList;
    }

    private List<TaskTemplate> getTaskTemplatesFromDB(TaskTemplateParams params, int pageSize, int preSize) {
        List<TaskTemplate> taskTemplateList = new ArrayList<>();
        if (params.getNodeType() != null && !"template".equals(params.getNodeType())) {
            return taskTemplateList;
        }
        params.setOffset(preSize);
        params.setLimit(pageSize);
        List<TaskTemplateDO> taskTemplateDOList = taskTemplateDAO.getTaskTemplateList(params);
        if (taskTemplateDOList == null) {
            return taskTemplateList;
        }
        for (TaskTemplateDO taskTemplateDO : taskTemplateDOList) {
            taskTemplateList.add(turnTaskTemplateDOToTaskTemplate(taskTemplateDO));
        }
        return taskTemplateList;
    }

    @NotNull
    private List<AbstractTaskRunner> getTaskRunners(TaskTemplateParams params) {
        List<AbstractTaskRunner> metaDataList = new ArrayList<>();
        TaskTemplateTypeEnum taskTemplateType = TaskTemplateTypeEnum.getEnumByType(params.getType());
        TaskCategory taskCategory = TaskCategory.getEnumByValue(params.getCategory());
        for (Map.Entry<String, AbstractTaskRunner> taskRunnerEntry: taskRunnerMap.entrySet()) {
            AbstractTaskRunner taskRunner = taskRunnerEntry.getValue();
            if (!taskRunner.isEnable() || params.getId() != null
                    || params.getName() != null && !taskRunner.getCategory().getValue().contains(params.getName())
                    || taskCategory != null && taskRunner.getCategory() != taskCategory
                    || taskTemplateType == TaskTemplateTypeEnum.PLUGIN
                    || taskTemplateType == TaskTemplateTypeEnum.FUNCTION && TaskCategory.FUNCTION != taskRunner.getCategory()
                    || taskTemplateType == TaskTemplateTypeEnum.LOGIC && TaskCategory.FUNCTION == taskRunner.getCategory()
            ) {
                continue;
            }
            metaDataList.add(taskRunner);
        }
        return metaDataList;
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
        result.setEnable(taskTemplateDO.getEnable());
        result.setTypeStr(TaskTemplateTypeEnum.getEnumByType(taskTemplateDO.getType()).getDesc());
        result.setNodeType("template");
        AbstractTaskRunner taskRunner = taskRunnerMap.get(taskTemplateDO.getCategory() + "TaskRunner");
        MetaData metaData = MetaData.builder().icon(taskRunner.getIcon()).fields(taskRunner.getFields()).build();
        result.setMetaData(metaData);
        return result;
    }

    private TaskTemplate turnMetaDataToTaskTemplate(AbstractTaskRunner taskRunner) {
        // 获取任务模板类型，由于元数据不存在插件类型，因此通过任务 category 二分为函数模板和逻辑模板
        TaskTemplateTypeEnum taskTemplateType = taskRunner.getCategory() == TaskCategory.FUNCTION
                ? TaskTemplateTypeEnum.FUNCTION: TaskTemplateTypeEnum.LOGIC;
        TaskTemplate result = new TaskTemplate();
        result.setId(null);
        result.setCategory(taskRunner.getCategory().getValue());
        result.setIcon(taskRunner.getIcon());
        result.setTaskYaml("");
        result.setName(taskRunner.getCategory().getValue());
        result.setOutput("{}");
        result.setSchema("{}");
        result.setEnable(1);
        result.setType(taskTemplateType.getType());
        result.setTypeStr(taskTemplateType.getDesc() + "（元数据）");
        result.setNodeType("meta");
        result.setMetaData(MetaData.builder().icon(taskRunner.getIcon()).fields(taskRunner.getFields()).build());
        return result;
    }

    public long createTaskTemplate(JSONObject taskTemplate) {
        try {
            TaskTemplateDO taskTemplateDO = JSONObject.parseObject(taskTemplate.toJSONString(), TaskTemplateDO.class);
            if (taskTemplateDO == null || taskTemplateDO.getName() == null || taskTemplateDO.getType() == null) {
                throw new IllegalArgumentException("task_template can't be null");
            }
            // set default value if field is null
            setTemplateDOBeforeCreate(taskTemplateDO);
            return taskTemplateDAO.insert(taskTemplateDO);
        } catch (Exception e) {
            log.warn("create task template error", e);
            throw e;
        }
    }

    private static void setTemplateDOBeforeCreate(TaskTemplateDO taskTemplateDO) {
        if (taskTemplateDO.getIcon() == null) {
            taskTemplateDO.setIcon("");
        }
        if (taskTemplateDO.getOutput() == null) {
            taskTemplateDO.setOutput("{}");
        }
        if (taskTemplateDO.getTaskYaml() == null) {
            taskTemplateDO.setTaskYaml("");
        }
        if (taskTemplateDO.getSchema() == null) {
            taskTemplateDO.setSchema("{}");
        }
        taskTemplateDO.setCreateTime(new Date());
        taskTemplateDO.setUpdateTime(new Date());
    }

    public int updateTaskTemplate(JSONObject taskTemplate) {
        try {
            TaskTemplateDO taskTemplateDO = JSONObject.parseObject(taskTemplate.toJSONString(), TaskTemplateDO.class);
            if (taskTemplateDO == null || taskTemplateDO.getId() == null) {
                throw new IllegalArgumentException("task_template and id can't be null");
            }
            taskTemplateDO.setUpdateTime(new Date());
            taskTemplateDO.setCreateTime(null);
            return taskTemplateDAO.update(taskTemplateDO);
        } catch (Exception e) {
            log.warn("update task template error", e);
            throw e;
        }
    }

    /**
     * 禁用任务模板
     * @param id 任务模板id
     * @return 更新条数
     */
    public int disableTaskTemplate(Long id) {
        try {
            return taskTemplateDAO.disable(id);
        } catch (Exception e) {
            log.warn("disable task template error, id: {}", id, e);
            throw e;
        }
    }

    /**
     * 启用任务模板
     * @param id 任务模板id
     * @return 更新条数
     */
    public int enableTaskTemplate(Long id) {
        try {
            return taskTemplateDAO.enable(id);
        } catch (Exception e) {
            log.warn("enable task template error, id: {}", id, e);
            throw e;
        }
    }
}
