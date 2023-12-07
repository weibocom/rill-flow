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

package com.weibo.rill.flow.service.component;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class DAGToolConverter {
    private static final Map<String, String> statusMapper = ImmutableMap.of("SUCCEED", "SUCCEEDED");
    public static Map<String, Object> convertTaskInfo(TaskInfo taskInfo) {
        Map<String, Object> ret = Maps.newHashMap();
        if (taskInfo == null) {
            return ret;
        }
        ret.put("name", taskInfo.getName());
        ret.put("status", Optional.ofNullable(statusMapper.get(taskInfo.getTaskStatus().name())).orElseGet(() -> taskInfo.getTaskStatus().name()));
        ret.put("contains_sub", Objects.equals(taskInfo.getTask().getCategory(), TaskCategory.FOREACH.getValue())
                || Objects.equals(taskInfo.getTask().getCategory(), TaskCategory.CHOICE.getValue()));
        ret.put("next", taskInfo.getNext().stream().map(TaskInfo::getName).collect(Collectors.toList()));
        ret.put("invoke_msg", taskInfo.getTaskInvokeMsg());
        ret.put("sub_group_index_to_status", taskInfo.getSubGroupIndexToStatus());
        ret.put("sub_group_key_judgement_mapping", taskInfo.getSubGroupKeyJudgementMapping());
        ret.put("sub_group_index_to_identity", taskInfo.getSubGroupIndexToIdentity());
        ret.put("task", taskInfo.getTask());
        return ret;
    }

    private DAGToolConverter() {

    }
}
