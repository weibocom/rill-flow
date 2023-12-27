package com.rill.plugin.dispatcher;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import org.pf4j.Extension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

@Extension
public class LocalTaskDispatcher implements DispatcherExtension {
    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        Map<String, Object> input = dispatchInfo.getInput();
        String cmd = input.get("cmd").toString();

        BufferedReader bufferedReader;
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                result.append(line).append("\n");
            }
            return new JSONObject(Map.of("result", result.toString())).toJSONString();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String getName() {
        return "local_task";
    }
}
