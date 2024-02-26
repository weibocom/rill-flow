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

package com.weibo.rill.flow.task.template.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.task.template.model.TaskTemplateParams;
import com.weibo.rill.flow.task.template.model.TaskTemplate;

import java.util.List;

/**
 * the service to manage task templates
 */
public interface TaskTemplateService {
    /**
     * get all meta data
     * @return meta data array
     */
    JSONArray getTaskMetaDataList();

    /**
     * get task templates by parameters
     * @param params task template parameters
     * @param page page number
     * @param pageSize records count per page
     * @return task template list
     */
    List<TaskTemplate> getTaskTemplates(TaskTemplateParams params, int page, int pageSize);

    /**
     * create a task template
     * @param taskTemplate task template to be created
     * @return task template id
     */
    long createTaskTemplate(JSONObject taskTemplate);

    /**
     * update a task template
     * @param taskTemplate task template to be updated
     * @return record count that has been affected
     */
    int updateTaskTemplate(JSONObject taskTemplate);
    /**
     * delete a task template
     * @param id task template id
     */
    int disableTaskTemplate(Long id);
    /**
     * enable a task template
     * @param id task template id
     */
    int enableTaskTemplate(Long id);
}
