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

package com.weibo.rill.flow.interfaces.model.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

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
public class TaskInvokeMsg {

    @JsonProperty("invoke_id")
    private String invokeId;

    @JsonProperty("code")
    private String code;

    @JsonProperty("msg")
    private String msg;

    @JsonProperty("input")
    private Map<String, Object> input;

    @JsonProperty("output")
    private Map<String, Object> output;

    @JsonProperty("referenced_dag_execution_id")
    private String referencedDAGExecutionId;

    @JsonProperty("invoke_time_infos")
    private List<InvokeTimeInfo> invokeTimeInfos;

    @JsonProperty("progress_args")
    private Map<String, Object> progressArgs;

    @JsonProperty("ext")
    private Map<String, Object> ext;

    public void updateInvokeMsg(TaskInvokeMsg taskInvokeMsg) {
        if (taskInvokeMsg == null) {
            return;
        }
        Optional.ofNullable(taskInvokeMsg.getInvokeId()).ifPresent(this::setInvokeId);
        Optional.ofNullable(taskInvokeMsg.getCode()).ifPresent(this::setCode);
        Optional.ofNullable(taskInvokeMsg.getMsg()).ifPresent(this::setMsg);
        Optional.ofNullable(taskInvokeMsg.getOutput()).ifPresent(this::setOutput);
        Optional.ofNullable(taskInvokeMsg.getReferencedDAGExecutionId()).ifPresent(this::setReferencedDAGExecutionId);
        Optional.ofNullable(taskInvokeMsg.getInvokeTimeInfos()).ifPresent(timeInfos -> {
            List<InvokeTimeInfo> timeInfoList = new ArrayList<>(timeInfos);
            if (invokeTimeInfos != null && !invokeTimeInfos.isEmpty()) {
                timeInfoList.addAll(invokeTimeInfos);
            }
            invokeTimeInfos = timeInfoList;
        });
        Optional.ofNullable(taskInvokeMsg.getExt()).filter(it -> !it.isEmpty()).ifPresent(extension -> {
            if (ext != null && !ext.isEmpty()) {
                ext.putAll(extension);
            } else {
                ext = extension;
            }
        });
    }

    public TaskInvokeMsg copy() {
        return TaskInvokeMsg.builder()
                .invokeId(invokeId)
                .code(code)
                .msg(msg)
                .referencedDAGExecutionId(referencedDAGExecutionId)
                .invokeTimeInfos(invokeTimeInfos)
                .ext(ext)
                .build();
    }
}
