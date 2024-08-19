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

import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import com.weibo.rill.flow.service.manager.DescriptorManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Service
public class FunctionTaskDispatcher implements DAGDispatcher {
    private final DescriptorManager descriptorManager;
    private final HttpInvokeHelper httpInvokeHelper;
    public static final Map<String, DispatcherExtension> protocolDispatcherMap = new ConcurrentHashMap<>();

    public FunctionTaskDispatcher(@Autowired @Qualifier("functionDispatcher") FunctionProtocolDispatcher functionDispatcher,
                                  @Autowired FlowProtocolDispatcher flowDispatcher,
                                  @Autowired @Qualifier("httpDispatcher") HttpProtocolDispatcher httpDispatcher,
                                  @Autowired @Qualifier("resourceDispatcher") ResourceProtocolDispatcher resourceDispatcher,
                                  @Autowired @Qualifier("resourceRefDispatcher") ResourceRefProtocolDispatcher resourceRefDispatcher,
                                  @Autowired @Qualifier("sseDispatcher") SseProtocolDispatcher sseDispatcher,
                                  @Autowired DescriptorManager descriptorManager,
                                  @Autowired HttpInvokeHelper httpInvokeHelper) {
        protocolDispatcherMap.put("function", functionDispatcher);
        protocolDispatcherMap.put("http", httpDispatcher);
        protocolDispatcherMap.put("https", httpDispatcher);
        protocolDispatcherMap.put("sse", sseDispatcher);
        protocolDispatcherMap.put("rillflow", flowDispatcher);
        protocolDispatcherMap.put("resource", resourceDispatcher);
        protocolDispatcherMap.put("resourceRef", resourceRefDispatcher);
        this.descriptorManager = descriptorManager;
        this.httpInvokeHelper = httpInvokeHelper;
    }

    @Override
    public String dispatch(DispatchInfo dispatchInfo) {
        try {
            FunctionTask functionTask = Optional.ofNullable(dispatchInfo)
                    .map(DispatchInfo::getTaskInfo)
                    .map(TaskInfo::getTask)
                    .filter(task -> task instanceof FunctionTask)
                    .map(task -> (FunctionTask) task)
                    .filter(task -> StringUtils.isNotBlank(task.getResourceName()) || task.getResource() != null)
                    .orElseThrow(() -> new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "handle functionPattern null"));

            HttpHeaders httpHeaders = new HttpHeaders();
            httpInvokeHelper.appendRequestHeader(httpHeaders, dispatchInfo.getExecutionId(), dispatchInfo.getTaskInfo(), dispatchInfo.getInput());
            dispatchInfo.setHeaders(httpHeaders);

            // fill parameters as default value to input
            Map<String, Object> input = dispatchInfo.getInput();
            Map<String, Object> parameters = functionTask.getParameters();
            if (parameters != null) {
                parameters.forEach(input::putIfAbsent);
            }

            if (functionTask.getResource() != null) {
                log.info("handle task by function resource, executionId:{} taskName:{}",
                        dispatchInfo.getExecutionId(), dispatchInfo.getTaskInfo().getName());
                return protocolDispatcherMap.get("resource").handle(null, dispatchInfo);
            }

            return resourceNameProcess(dispatchInfo, functionTask);
        } catch (TaskException e) {
            throw e;
        } catch (Exception e) {
            log.warn("handle function task resource fails, dispatchInfo:{}", dispatchInfo, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "handle fails: " + e.getMessage(), e.getCause());
        }
    }

    private String resourceNameProcess(DispatchInfo dispatchInfo, FunctionTask functionTask) {
        String executionId = dispatchInfo.getExecutionId();
        Map<String, Object> input = dispatchInfo.getInput();
        Resource resource = new Resource(functionTask.getResourceName(), functionTask.getResourceProtocol());

        if (resource.isAbResource()) {
            Long uid = Optional.ofNullable(input.get("uid"))
                    .map(it -> Long.parseLong(String.valueOf(it)))
                    .orElse(0L);
            String calculatedResourceName = descriptorManager.calculateResourceName(uid, input, executionId, resource.getSchemeValue());
            resource = new Resource(calculatedResourceName);
            updateResourceName(executionId, calculatedResourceName, dispatchInfo.getTaskInfo());
        }
        log.info("handle task executionId:{} resourceName:{} taskInfoName:{} ", executionId, resource.getResourceName(), dispatchInfo.getTaskInfo().getName());

        DispatcherExtension protocolDispatcher = protocolDispatcherMap.get(resource.getSchemeProtocol());
        if (protocolDispatcher == null) {
            log.warn("handle function scheme protocol:{} do not support", resource.getSchemeProtocol());
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "handle functionPattern protocol: " + resource.getSchemeProtocol() + "do not support");
        }
        return protocolDispatcher.handle(resource, dispatchInfo);
    }

    private void updateResourceName(String executionId, String calculatedResourceName, TaskInfo taskInfo) {
        try {
            TaskInvokeMsg taskInvokeMsg = Optional.ofNullable(taskInfo.getTaskInvokeMsg()).orElseGet(() -> {
                taskInfo.setTaskInvokeMsg(new TaskInvokeMsg());
                return taskInfo.getTaskInvokeMsg();
            });
            Map<String, Object> ext = Optional.ofNullable(taskInvokeMsg.getExt()).orElseGet(() -> {
                taskInvokeMsg.setExt(new HashMap<>());
                return taskInvokeMsg.getExt();
            });
            ext.put("calculated_resource_name", calculatedResourceName);
        } catch (Exception e) {
            log.warn("updateResourceName fails, executionId:{}, calculatedResourceName:{}, taskInfoName:{}, errorMsg:{}",
                    executionId, calculatedResourceName, Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null), e.getMessage());
        }
    }
}
