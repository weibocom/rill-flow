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
            metaData.put("name", taskRunnerEntry.getKey());
            metaDataList.add(metaData);
        }
        return metaDataList;
    }
}
