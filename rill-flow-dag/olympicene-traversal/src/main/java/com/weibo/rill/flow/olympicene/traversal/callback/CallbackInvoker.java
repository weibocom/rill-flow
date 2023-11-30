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

import com.weibo.rill.flow.olympicene.core.event.Callback;
import com.weibo.rill.flow.olympicene.core.event.Event;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;

/**
 * 注:
 * callback调用时使用当时上下文中的dagInfo和context信息，不对核心逻辑造成额外的负担。
 * 如果业务需要全量的context或者dagInfo，可以通过引入额外的CallbackListener实现。
 * 外额外的listener中读取资源从库来获取全量数据。
 */
@Slf4j
public class CallbackInvoker {
    private static final CallbackInvoker INSTANCE = new CallbackInvoker();

    synchronized public static CallbackInvoker getInstance() {
        return INSTANCE;
    }

    public void callback(Callback<DAGCallbackInfo> callback, DAGEvent dagEvent,
                         DAGInfo dagInfo, Map<String, Object> context, TaskInfo taskInfo) {
        if (callback == null || dagEvent == null) {
            return;
        }

        try {
            Event<DAGCallbackInfo> event = Event.<DAGCallbackInfo>builder()
                    .timestamp(System.currentTimeMillis())
                    .id(dagInfo.getExecutionId())
                    .eventCode(dagEvent.getCode())
                    .data(DAGCallbackInfo.builder()
                            .executionId(dagInfo.getExecutionId())
                            .dagInfo(dagInfo)
                            .context(context)
                            .taskInfo(taskInfo)
                            .build())
                    .build();

            callback.onEvent(event);
        } catch (Throwable e) {
            log.error("callback error, dagEvent:{}, executionId:{}, taskInfoName:{}",
                    dagEvent,
                    Optional.ofNullable(dagInfo).map(DAGInfo::getExecutionId).orElse(null),
                    Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null),
                    e);
        }
    }

    private CallbackInvoker() {

    }
}
