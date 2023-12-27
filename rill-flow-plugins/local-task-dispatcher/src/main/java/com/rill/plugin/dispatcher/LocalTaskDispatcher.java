package com.rill.plugin.dispatcher;

import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class LocalTaskDispatcher implements DispatcherExtension {
    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        Map<String, Object> input = dispatchInfo.getInput();
        String cmd = input.get("cmd").toString();

        BufferedReader br;
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String getName() {
        return "local_task";
    }
}
