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

package com.weibo.rill.flow.trigger.util;

import com.alibaba.fastjson.JSONObject;

/**
 * @author moqi
 * Create on 2023/12/14 17:31
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
