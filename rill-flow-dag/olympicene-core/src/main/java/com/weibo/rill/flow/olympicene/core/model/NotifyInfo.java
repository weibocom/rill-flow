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

package com.weibo.rill.flow.olympicene.core.model;

import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.olympicene.core.model.strategy.RetryContext;
import com.weibo.rill.flow.interfaces.model.task.FunctionPattern;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class NotifyInfo {
    private String parentTaskInfoName;

    private String taskInfoName;

    private List<String> taskInfoNames;

    private TaskStatus taskStatus;

    private String completedGroupIndex;

    private TaskStatus groupTaskStatus;

    private TaskInvokeMsg taskInvokeMsg;

    private Map<String, TaskInfo> tasks;

    private String parentDAGExecutionId;

    private String parentDAGTaskInfoName;

    private FunctionPattern parentDAGTaskExecutionType;

    private RetryContext retryContext;

    private CallbackConfig callbackConfig;

    private Map<String, Object> ext;
}
