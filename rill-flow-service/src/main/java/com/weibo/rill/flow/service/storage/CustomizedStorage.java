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

package com.weibo.rill.flow.service.storage;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;


public interface CustomizedStorage {
    String initBucket(String bucketName, JSONObject fieldToValues);

    void store(String bucketName, JSONObject fieldToValues);

    Map<String, String> load(String bucketName, boolean hGetAll, List<String> fieldNames, String fieldPrefix);

    boolean remove(String bucketName);

    boolean remove(String bucketName, List<String> fieldNames);
}
