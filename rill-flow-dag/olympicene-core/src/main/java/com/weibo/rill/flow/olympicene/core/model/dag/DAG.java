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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.interfaces.model.strategy.Timeline;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
public class DAG {
    private String workspace;
    private String dagName;
    private String version;
    private DAGType type;
    private Timeline timeline;
    private List<BaseTask> tasks;
    @Setter
    private List<BaseResource> resources;
    private CallbackConfig callbackConfig;
    @Setter
    private Map<String, String> defaultContext;
    @Setter
    private Map<String, List<Mapping>> commonMapping;
    private String inputSchema;

    @JsonCreator
    public DAG(@JsonProperty("workspace") String workspace,
               @JsonProperty("dagName") String dagName,
               @JsonProperty("version") String version,
               @JsonProperty("type") DAGType type,
               @JsonProperty("timer") Timeline timeline,
               @JsonProperty("tasks") List<BaseTask> tasks,
               @JsonProperty("resources") List<BaseResource> resources,
               @JsonProperty("callback") CallbackConfig callbackConfig,
               @JsonProperty("defaultContext") Map<String, String> defaultContext,
               @JsonProperty("commonMapping") Map<String, List<Mapping>> commonMapping,
               @JsonProperty("namespace") String namespace,
               @JsonProperty("service") String service,
               @JsonProperty("inputSchema") String inputSchema) {
        this.workspace = StringUtils.isBlank(workspace) ? namespace : workspace;
        this.dagName = StringUtils.isBlank(dagName) ? service : dagName;
        this.version = version;
        this.type = type;
        this.timeline = timeline;
        this.tasks = tasks;
        this.resources = resources;
        this.callbackConfig = callbackConfig;
        this.defaultContext = defaultContext;
        this.commonMapping = commonMapping;
        this.inputSchema = inputSchema;
    }
}
