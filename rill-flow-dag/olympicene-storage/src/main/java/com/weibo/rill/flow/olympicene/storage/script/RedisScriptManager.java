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

package com.weibo.rill.flow.olympicene.storage.script;

import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.exception.StorageException;
import com.weibo.rill.flow.olympicene.storage.redis.lock.ResourceLoader;

import java.io.IOException;

public class RedisScriptManager {
    private static final String REDIS_SET_WITH_EXPIRE;
    private static final String REDIS_GET;
    private static final String REDIS_GET_BY_FIELD_AND_KEY;
    private static final String REDIS_EXPIRE;
    private static final String DAG_INFO_SET;
    private static final String DAG_INFO_GET;
    private static final String DAG_INFO_GET_BY_FIELD;

    static {
        try {
            REDIS_SET_WITH_EXPIRE = ResourceLoader.loadResourceAsText("lua/redis_set_with_expire.lua");
            REDIS_GET = ResourceLoader.loadResourceAsText("lua/redis_get.lua");
            REDIS_GET_BY_FIELD_AND_KEY = ResourceLoader.loadResourceAsText("lua/redis_get_by_field_and_key.lua");
            REDIS_EXPIRE = ResourceLoader.loadResourceAsText("lua/redis_expire.lua");
            DAG_INFO_SET = ResourceLoader.loadResourceAsText("lua/dag_info_set.lua");
            DAG_INFO_GET = ResourceLoader.loadResourceAsText("lua/dag_info_get.lua");
            DAG_INFO_GET_BY_FIELD = ResourceLoader.loadResourceAsText("lua/dag_info_get_by_field.lua");
        } catch (IOException e) {
            throw new StorageException(StorageErrorCode.RESOURCE_NOT_FOUND.getCode(), StorageErrorCode.RESOURCE_NOT_FOUND.getMessage());
        }
    }

    public static String getRedisSetWithExpire() {
        return REDIS_SET_WITH_EXPIRE;
    }

    public static String getRedisGet() {
        return REDIS_GET;
    }

    public static String getRedisGetByFieldAndKey() {
        return REDIS_GET_BY_FIELD_AND_KEY;
    }

    public static String getRedisExpire() {
        return REDIS_EXPIRE;
    }

    public static String dagInfoSetScript() {
        return DAG_INFO_SET;
    }

    public static String dagInfoGetScript() {
        return DAG_INFO_GET;
    }

    public static String dagInfoGetByFieldScript() {
        return DAG_INFO_GET_BY_FIELD;
    }

}
