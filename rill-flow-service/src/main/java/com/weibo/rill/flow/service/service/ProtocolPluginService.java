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

package com.weibo.rill.flow.service.service;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static com.weibo.rill.flow.service.plugin.PluginLoader.TASK_EXTENSION_SET;

@Service
public class ProtocolPluginService {

    public List<JSONObject> getProtocolPlugins() {
        List<JSONObject> result = new ArrayList<>();
        for (DispatcherExtension dispatcherExtension : TASK_EXTENSION_SET) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name", dispatcherExtension.getName());
            if (dispatcherExtension.getIcon() != null) {
                jsonObject.put("icon", dispatcherExtension.getIcon());
            }
            if (dispatcherExtension.getSchema() != null) {
                jsonObject.put("schema", dispatcherExtension.getSchema());
            }
            result.add(jsonObject);
        }
        return result;
    }
}
