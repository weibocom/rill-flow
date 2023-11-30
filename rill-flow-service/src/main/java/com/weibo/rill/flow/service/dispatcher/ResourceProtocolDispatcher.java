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
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Service("resourceDispatcher")
public class ResourceProtocolDispatcher implements DispatcherExtension {
    public static final Map<String, ResourceDispatcher> resourceDispatcherMap = new ConcurrentHashMap<>();

    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        FunctionTask functionTask = (FunctionTask) dispatchInfo.getTaskInfo().getTask();
        BaseResource baseResource = functionTask.getResource();
        if (baseResource == null) {
            log.info("function resource null, executionId:{} taskName:{}", dispatchInfo.getExecutionId(), dispatchInfo.getTaskInfo().getName());
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "function resource null");
        }

        ResourceDispatcher resourceDispatcher = resourceDispatcherMap.get(baseResource.getCategory());
        if (resourceDispatcher == null) {
            log.warn("function resource category:{} do not support", baseResource.getCategory());
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "function resource category: " + baseResource.getCategory() + "do not support");
        }
        return resourceDispatcher.dispatch(dispatchInfo);
    }

    @Override
    public String getName() {
        return "resource";
    }
}
