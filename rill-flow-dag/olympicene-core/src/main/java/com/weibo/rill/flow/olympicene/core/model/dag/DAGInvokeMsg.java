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

package com.weibo.rill.flow.olympicene.core.model.dag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.interfaces.model.task.FunctionPattern;
import com.weibo.rill.flow.interfaces.model.task.InvokeTimeInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import lombok.*;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class DAGInvokeMsg {
    @JsonProperty("dag_execution_routes")
    private List<ExecutionInfo> executionRoutes;

    @JsonProperty("code")
    private String code;

    @JsonProperty("msg")
    private String msg;

    @JsonProperty("callback")
    private CallbackConfig callbackConfig;

    @JsonProperty("invoke_time_infos")
    private List<InvokeTimeInfo> invokeTimeInfos;

    @JsonProperty("ext")
    private Map<String, Object> ext;

    public void updateInvokeMsg(DAGInvokeMsg dagInvokeMsg) {
        if (dagInvokeMsg == null) {
            return;
        }

        Optional.ofNullable(dagInvokeMsg.getExecutionRoutes()).ifPresent(this::setExecutionRoutes);
        Optional.ofNullable(dagInvokeMsg.getMsg()).ifPresent(this::setMsg);
        Optional.ofNullable(dagInvokeMsg.getInvokeTimeInfos()).ifPresent(timeInfos -> {
            List<InvokeTimeInfo> timeInfoList = Lists.newArrayList(timeInfos);
            if (CollectionUtils.isNotEmpty(invokeTimeInfos)) {
                timeInfoList.addAll(invokeTimeInfos);
            }
            invokeTimeInfos = timeInfoList;
        });
    }

    public void updateInvokeMsg(TaskInvokeMsg taskInvokeMsg) {
        Optional.ofNullable(taskInvokeMsg.getCode()).ifPresent(this::setCode);
        Optional.ofNullable(taskInvokeMsg.getMsg()).ifPresent(this::setMsg);
        Optional.ofNullable(taskInvokeMsg.getExt()).ifPresent(this::setExt);
    }

    public static DAGInvokeMsg cloneToSave(DAGInvokeMsg dagInvokeMsg) {
        if (dagInvokeMsg == null) {
            return null;
        }

        List<ExecutionInfo> executionInfos = new ArrayList<>();
        Optional.ofNullable(dagInvokeMsg.getExecutionRoutes()).ifPresent(executionInfos::addAll);
        return DAGInvokeMsg.builder()
                .executionRoutes(executionInfos)
                .code(dagInvokeMsg.getCode())
                .msg(dagInvokeMsg.getMsg())
                .callbackConfig(dagInvokeMsg.getCallbackConfig())
                .invokeTimeInfos(dagInvokeMsg.getInvokeTimeInfos())
                .ext(dagInvokeMsg.getExt())
                .build();
    }

    @Getter
    @Setter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ExecutionInfo {
        private int index;

        @JsonProperty("execution_id")
        private String executionId;

        @JsonProperty("task_info_name")
        private String taskInfoName;

        @JsonProperty("execution_type")
        private FunctionPattern executionType;
    }
}
