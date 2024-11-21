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

package com.weibo.rill.flow.service.dispatcher;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.core.model.DAGSettings;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.traversal.Olympicene;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.service.DAGDescriptorService;
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.service.util.ProfileActions;
import com.weibo.rill.flow.service.util.PrometheusActions;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;


@Slf4j
@Service
public class FlowProtocolDispatcher implements DispatcherExtension {
    @Autowired
    private DAGDescriptorService dagDescriptorService;
    @Autowired
    private BizDConfs bizDConfs;
    @Setter
    private Olympicene olympicene;
    @Autowired
    private DAGResourceStatistic dagResourceStatistic;

    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        String parentDAGExecutionId = dispatchInfo.getExecutionId();
        String parentTaskName = dispatchInfo.getTaskInfo().getName();
        NotifyInfo notifyInfo = NotifyInfo.builder()
                .parentDAGExecutionId(parentDAGExecutionId)
                .parentDAGTaskInfoName(parentTaskName)
                .parentDAGTaskExecutionType(((FunctionTask) dispatchInfo.getTaskInfo().getTask()).getPattern())
                .build();

        Map<String, Object> data = Maps.newHashMap();
        Optional.ofNullable(dispatchInfo.getInput()).ifPresent(data::putAll);
        Long uid = Optional.ofNullable(data.get("uid")).map(it -> Long.parseLong(String.valueOf(it))).orElse(0L);
        DAG dag = dagDescriptorService.getDAG(uid, data, resource.getSchemeValue());
        String executionId = ExecutionIdUtil.generateExecutionId(dag);
        data.put("flow_execution_id", executionId);
        DAGSettings dagSettings = DAGSettings.builder()
                .ignoreExist(false)
                .dagMaxDepth(bizDConfs.getFlowDAGMaxDepth()).build();
        olympicene.submit(executionId, null, dag, data, dagSettings, notifyInfo);
        dagResourceStatistic.updateFlowTypeResourceStatus(parentDAGExecutionId, parentTaskName, resource.getResourceName(), dag);
        ProfileActions.recordTinyDAGSubmit(executionId);
        // 记录prometheus
        PrometheusActions.recordTinyDAGSubmit(executionId);

        log.info("submitFlow bigExecutionId:{} tinyExecutionId:{}", parentDAGExecutionId, executionId);
        return JSON.toJSONString(ImmutableMap.of("execution_id", executionId));
    }

    @Override
    public String getName() {
        return "rill-flow";
    }
}
