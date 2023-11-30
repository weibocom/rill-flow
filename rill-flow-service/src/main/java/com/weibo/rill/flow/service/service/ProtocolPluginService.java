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
