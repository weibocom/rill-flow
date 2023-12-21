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

package com.weibo.rill.flow.trigger.controller;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.trigger.triggers.Trigger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Optional;

@RestController
@Slf4j
@RequestMapping("/flow/trigger")
public class TriggerController {
    @Autowired
    private Map<String, Trigger> triggerMap;

    @PostConstruct
    private void initTriggers() {
        for (Map.Entry<String, Trigger> triggerEntry: triggerMap.entrySet()) {
            try {
                triggerEntry.getValue().initTriggerTasks();
            } catch (Exception e) {
                log.warn("init trigger error, trigger type: {}, ", triggerEntry.getKey(), e);
            }
        }
    }

    @PostMapping("/add_trigger.json")
    public JSONObject addTrigger(User flowUser, @RequestParam("type") String type,
                                 @RequestParam(value = "descriptor_id") String descriptorId,
                                 @RequestParam(value = "callback", required = false) String callback,
                                 @RequestParam(value = "resource_check", required = false) String resourceCheck,
                                 @RequestBody(required = false) JSONObject body) {
        Long uid = Optional.ofNullable(flowUser).map(User::getUid).orElse(0L);
        Trigger trigger = triggerMap.get(type + "_trigger");
        if (trigger == null) {
            throw new TaskException(BizError.ERROR_PROCESS_FAIL, "do not support type: " + type);
        }
        return trigger.addTriggerTask(uid, descriptorId, callback, resourceCheck, body);
    }

    @PostMapping("/cancel_trigger.json")
    public JSONObject cancelTrigger(User flowUser, @RequestParam("type") String type, @RequestParam("task_id") String taskId) {
        Trigger trigger = triggerMap.get(type + "_trigger");
        if (trigger == null) {
            throw new TaskException(BizError.ERROR_PROCESS_FAIL, "do not support type: " + type);
        }
        boolean res = trigger.cancelTriggerTask(taskId);
        return new JSONObject(Map.of("code", res?0: -1));
    }

    @GetMapping("/get_trigger_tasks.json")
    public JSONObject getTriggerTasks(User flowUser, @RequestParam("type") String type) {
        Trigger trigger = triggerMap.get(type + "_trigger");
        if (trigger == null) {
            return new JSONObject(Map.of("code", -1, "message", "do not support type: " + type));
        }
        return new JSONObject(Map.of("code", 0, "data", trigger.getTriggerTasks()));
    }
}
