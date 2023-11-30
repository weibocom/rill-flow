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

package com.weibo.rill.flow.impl.switcher;

import java.util.concurrent.atomic.AtomicBoolean;

public class Switchers {
    public static final AtomicBoolean ENABLE_DAG_CONTEXT_LENGTH_CHECK = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_DAG_INFO_LENGTH_CHECK = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_FUNCTION_DISPATCH_RET_CHECK = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_RUNTIME_STORAGE_USAGE_CHECK = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_RUNTIME_RESOURCE_STATUS_CHECK = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_RUNTIME_SUBMIT_TRAFFIC_CONTROL = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_RUNTIME_SUBMIT_DAG_CHECK = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_GET_DESCRIPTOR_FROM_CACHE = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_AVIATOR_COMPILE_EXPRESSION_CACHE = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_TENANT_TASK_FLOW_AGGREGATE = new AtomicBoolean(false);
    public static final AtomicBoolean ENABLE_TENANT_TASK_BUSINESS_AGGREGATE = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_FLOW_DAG_MULTI_REDO = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_FLOW_CONCURRENT_TASK_INDEPENDENT_CONTEXT = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_THREAD_ISOLATION = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_RECORD_COMPLIANCE_WHEN_TASK_FINISHED = new AtomicBoolean(false);
    public static final AtomicBoolean ENABLE_FLOW_STASH = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_FLOW_STASH_SCHEDULED_TASK_POP = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_FLOW_STASH_SCHEDULED_RESOURCE_SCORE_FETCH = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_EXTREME_STRATEGY_WITH_FLOW_RESOURCE_SCORE = new AtomicBoolean(false);
    public static final AtomicBoolean ENABLE_CONSERVATIVE_STRATEGY_WITH_FLOW_RESOURCE_SCORE = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_AB_RESOURCE_WHEN_STASH = new AtomicBoolean(true);
    public static final AtomicBoolean ENABLE_UPDATE_TASK_SCORE_WHEN_POP = new AtomicBoolean(false);

    public static final AtomicBoolean ENABLE_AUTH_RESOLVER = new AtomicBoolean(false);

    public static final AtomicBoolean ENABLE_OPEN_PROMETHEUS = new AtomicBoolean(true);

    public static final AtomicBoolean ENABLE_SET_INPUT_OUTPUT = new AtomicBoolean(true);


    private Switchers() {

    }
}
