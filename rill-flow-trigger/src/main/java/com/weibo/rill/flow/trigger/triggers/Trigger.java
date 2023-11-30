package com.weibo.rill.flow.trigger.triggers;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public interface Trigger {
    void initTriggerTasks();
    JSONObject addTriggerTask(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject body);
    boolean cancelTriggerTask(String taskId);
    Map<String, JSONObject> getTriggerTasks();
}
