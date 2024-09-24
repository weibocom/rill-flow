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

package com.weibo.api.flow.executors.model.constant;

public class SseConstant {
    private SseConstant() {}
    public static final long SSE_EMITTER_TIMEOUT = 0L; // 传递 0 表示不限制
    public static final int REDIS_EXPIRE = 24 * 60 * 60;
    public static final int SSE_DATA_GET_INTERVAL = 100;
    public static final String SSE_MARK = "data:";
}
