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

package com.weibo.rill.flow.olympicene.core.model.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.strategy.Degrade;
import com.weibo.rill.flow.interfaces.model.strategy.Progress;
import com.weibo.rill.flow.interfaces.model.strategy.Timeline;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

@Getter
@Setter
@JsonTypeName("switch")
public class SwitchTask extends BaseTask {

    List<Switch> switches;

    @JsonCreator
    public SwitchTask(
            @JsonProperty("name") String name,
            @JsonProperty("title") String title,
            @JsonProperty("description") String description,
            @JsonProperty("category") String category,
            @JsonProperty("inputMappings") List<Mapping> inputMappings,
            @JsonProperty("switches") List<Switch> switches,
            @JsonProperty("progress") Progress progress,
            @JsonProperty("degrade") Degrade degrade,
            @JsonProperty("timeline") Timeline timeline,
            @JsonProperty("isKeyCallback") boolean isKeyCallback,
            @JsonProperty("keyExp") String keyExp,
            @JsonProperty("parameters") Map<String, Object> parameters,
            @JsonProperty("templateId") String templateId) {
        super(name, title, description, category, null, false, inputMappings, null, progress,
                degrade, timeline, isKeyCallback, keyExp, parameters, templateId);
        this.switches = switches;
        this.setNext(getNext(name, switches));
    }

    private static String getNext(String taskName, List<Switch> switches) {
        if (CollectionUtils.isEmpty(switches)) {
            return "";
        }
        DAGWalkHelper dagWalkHelper = DAGWalkHelper.getInstance();
        boolean isAncestorTask = dagWalkHelper.isAncestorTask(taskName);
        if (isAncestorTask) {
            return switches.stream().map(Switch::getNext).collect(Collectors.joining(","));
        }
        String rootName = dagWalkHelper.getRootName(taskName);
        Set<String[]> nextArrays = switches.stream().map(it -> it.getNext().split(",")).collect(Collectors.toSet());
        Set<String> nextTaskNames = new HashSet<>();
        for (String[] nextArray : nextArrays) {
            Set<String> nextNames = Arrays.stream(nextArray).map(String::trim).filter(StringUtils::isNotBlank)
                    .map(it -> dagWalkHelper.buildTaskInfoName(rootName, it))
                    .collect(Collectors.toSet());
            nextTaskNames.addAll(nextNames);
        }
        return String.join(",", nextTaskNames);
    }

    @Override
    public List<BaseTask> subTasks() {
        return new ArrayList<>();
    }
}
