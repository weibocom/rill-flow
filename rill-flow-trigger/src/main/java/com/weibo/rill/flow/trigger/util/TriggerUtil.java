package com.weibo.rill.flow.trigger.util;

import com.alibaba.fastjson.JSONObject;

/**
 * @author moqi
 * @date 2023/12/14 17:31
 */
public class TriggerUtil {

    public static JSONObject buildCommonDetail(Long uid, String descriptorId, String callback, String resourceCheck) {
        JSONObject jsonDetails = new JSONObject();
        jsonDetails.put("descriptor_id", descriptorId);
        jsonDetails.put("uid", uid);
        if (jsonDetails.containsKey("callback")) {
            jsonDetails.put("callback", callback);
        }
        if (jsonDetails.containsKey("resource_check")) {
            jsonDetails.put("resource_check", resourceCheck);
        }
        return jsonDetails;
    }

}
