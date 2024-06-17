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

package com.weibo.rill.flow.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.service.facade.DAGRuntimeFacade;
import io.swagger.annotations.Api;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@Api(tags = {"工作流编排结果获取接口"})
@RequestMapping("/flow")
public class DAGRuntimeController {
    private static final String EXECUTION_ID = "execution_id";

    @Autowired
    private DAGRuntimeFacade dagRuntimeFacade;

    @RequestMapping(value = "complete_dag.json", method = RequestMethod.POST)
    public Map<String, String> completeDAG(User flowUser,
                                           @RequestParam(value = EXECUTION_ID) String executionId,
                                           @RequestParam(value = "success", defaultValue = "true", required = false) boolean success) {
        try {
            boolean ret = dagRuntimeFacade.updateDagStatus(executionId, success ? DAGStatus.SUCCEED : DAGStatus.FAILED);
            return ImmutableMap.of("code", "0", "message", ret ? "ok" : "failed");
        } catch (Exception e) {
            return ImmutableMap.of("code", "-1", "message", e.getMessage());
        }
    }

    @RequestMapping(value = "service_check.json", method = RequestMethod.GET)
    public Map<String, String> serviceCheck() {
        return ImmutableMap.of("ret", "ok");
    }

    @RequestMapping(value = "get.json", method = RequestMethod.GET)
    public Map<String, Object> get(User flowUser,
                                   @RequestParam(value = EXECUTION_ID) String executionId,
                                   @RequestParam(value = "brief", defaultValue = "false") boolean brief) {
        return ImmutableMap.of("ret", dagRuntimeFacade.getBasicDAGInfo(executionId, brief));
    }

    @RequestMapping(value = "get_by_parent.json", method = RequestMethod.GET)
    public Map<String, Object> getByParent(User flowUser,
                                           @RequestParam(value = EXECUTION_ID) String executionId,
                                           @RequestParam(value = "parent") String parent,
                                           @RequestParam(value = "group_index") String groupIndex) {
        return ImmutableMap.of("ret", dagRuntimeFacade.getDAGInfoByParentName(executionId, parent, groupIndex));
    }

    @RequestMapping(value = "convert.json", method = RequestMethod.POST)
    public Map<String, Object> convert(User flowUser,
                                       @RequestBody(required = false) String dagDescriptor) {
        return ImmutableMap.of("ret", dagRuntimeFacade.convertDAGInfo(dagDescriptor));
    }

    @RequestMapping(value = "get_context.json", method = RequestMethod.GET)
    public Map<String, Object> getContext(User flowUser,
                                          @RequestParam(value = EXECUTION_ID, required = false) String executionId) {
        if (StringUtils.isEmpty(executionId)) {
            return ImmutableMap.of("code", -1, "msg", "executionId is empty!");
        }

        return ImmutableMap.of("ret", dagRuntimeFacade.getContext(executionId, null));
    }

    @RequestMapping(value = "get_sub_context.json", method = RequestMethod.GET)
    public Map<String, Object> getSubContext(User flowUser,
                                             @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                             @RequestParam(value = "parent", required = false) String parentTaskName,
                                             @RequestParam(value = "group_index", required = false) Integer groupIndex) {

        if (StringUtils.isEmpty(executionId) || StringUtils.isEmpty(parentTaskName) || groupIndex == null) {
            return ImmutableMap.of("code", -1, "msg", "executionId or parent is empty or missing group_index!");
        }

        return ImmutableMap.of("ret", dagRuntimeFacade.getSubContext(executionId, parentTaskName, groupIndex));
    }

    @RequestMapping(value = "mapping_evaluation.json", method = RequestMethod.POST)
    public Map<String, Object> outputMappingEvaluation(User flowUser,
                                                       @RequestParam(value = "type", required = false) String type,
                                                       @RequestParam(value = "mapping_rules", required = false) String mappingRules,
                                                       @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                                       @RequestParam(value = "task_name", required = false) String taskName,
                                                       @RequestBody(required = false) JSONObject data) {
        data = Optional.ofNullable(data).orElse(new JSONObject());
        return ImmutableMap.of("ret", dagRuntimeFacade.mappingEvaluation(type, mappingRules, executionId, taskName, data));
    }

    @RequestMapping(value = "function_dispatch_params.json", method = RequestMethod.POST)
    public Map<String, Object> functionDispatchParams(User flowUser,
                                                      @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                                      @RequestParam(value = "task_name", required = false) String taskName,
                                                      @RequestBody(required = false) JSONObject data) {
        data = Optional.ofNullable(data).orElse(new JSONObject());
        return ImmutableMap.of("ret", dagRuntimeFacade.functionDispatchParams(executionId, taskName, data));
    }

    @RequestMapping(value = "dependency_check.json", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> dependencyCheck(User flowUser,
                                               @RequestParam(value = "descriptor_id", required = false) String descriptorId,
                                               @RequestBody(required = false) String descriptor) {
        return ImmutableMap.of("ret", dagRuntimeFacade.dependencyCheck(descriptorId, descriptor));
    }

    @RequestMapping(value = "runtime_dependent_resources.json", method = RequestMethod.GET)
    public Map<String, Object> runtimeDependentResources(User flowUser,
                                                         @RequestParam(value = "service_ids") List<String> serviceIds) {

        if (CollectionUtils.isEmpty(serviceIds)) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "service_ids cannot empty");
        }

        return dagRuntimeFacade.runtimeDependentResources(serviceIds);
    }

    @RequestMapping(value = "clear_runtime_resources.json", method = RequestMethod.POST)
    public Map<String, Object> clearRuntimeResources(User flowUser,
                                                     @RequestParam(value = "service_id") String serviceId,
                                                     @RequestParam(value = "clear_all", defaultValue = "false") boolean clearAll,
                                                     @RequestParam(value = "resource_names", required = false) List<String> resourceNames) {
        return dagRuntimeFacade.clearRuntimeResources(serviceId, clearAll, resourceNames);
    }

    @RequestMapping(value = "business_invoke_summary.json", method = RequestMethod.GET)
    public Map<String, Object> businessInvokeSummary(User flowUser,
                                                     @RequestParam(value = "business_key") String businessKey) {
        return dagRuntimeFacade.businessInvokeSummary(businessKey);
    }
}