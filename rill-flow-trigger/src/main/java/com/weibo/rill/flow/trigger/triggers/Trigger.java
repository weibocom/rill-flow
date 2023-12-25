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

package com.weibo.rill.flow.trigger.triggers;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public interface Trigger {
    void initTriggerTasks();
    JSONObject addTriggerTask(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject body);
    boolean cancelTriggerTask(String taskId);
    Map<String, JSONObject> getTriggerTasks();
}
