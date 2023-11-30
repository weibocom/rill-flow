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

package com.weibo.rill.flow.olympicene.traversal.callback;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum DAGEvent {
    DAG_FAILED(1000, "DAG_FAILED"),
    DAG_SUCCEED(1001, "DAG_SUCCEED"),
    TASK_FINISH(1002, "TASK_FINISH"),
    TASK_FAILED(1003, "TASK_FAILED"),
    TASK_SKIPPED(1004, "TASK_SKIPPED"),
    DAG_KEY_SUCCEED(1005, "DAG_KEY_SUCCEED"),
    ;

    private final int code;
    private final String msg;
}
