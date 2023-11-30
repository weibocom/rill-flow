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

package com.weibo.rill.flow.olympicene.storage.constant;

public enum DAGRedisPrefix {
    PREFIX_CONTEXT("context_"),
    PREFIX_CONTEXT_MAPPING("context_mapping_"),
    PREFIX_SUB_CONTEXT("sub_context_"),
    PREFIX_DAG_INFO("dag_info_"),
    PREFIX_DAG_DESCRIPTOR("dag_descriptor_"),
    PREFIX_SUB_TASK_MAPPING("sub_task_mapping_"),
    PREFIX_SUB_TASK("sub_task_")
    ;

    private final String value;

    DAGRedisPrefix(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
