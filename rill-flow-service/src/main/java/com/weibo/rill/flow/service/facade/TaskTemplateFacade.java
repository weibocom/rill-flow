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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
            metaData.put("category", taskRunner.getCategory());
            metaData.put("icon", taskRunner.getIcon());
            metaData.put("fields", taskRunner.getFields());
            metaDataList.add(metaData);
        }
        return metaDataList;
    }
}
