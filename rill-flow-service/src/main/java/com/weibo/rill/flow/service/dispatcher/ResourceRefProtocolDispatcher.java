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

import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.service.manager.DescriptorManager;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;


@Slf4j
@Service("resourceRefDispatcher")
public class ResourceRefProtocolDispatcher implements DispatcherExtension {
    @Autowired
    private DescriptorManager descriptorManager;
    @Autowired
    private ResourceProtocolDispatcher resourceProtocolDispatcher;

    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        Map<String, Object> data =Optional.ofNullable(dispatchInfo.getInput()).orElse(Maps.newHashMap());
        Long uid = Optional.ofNullable(data.get("uid")).map(it -> Long.parseLong(String.valueOf(it))).orElse(0L);
        BaseResource baseResource = descriptorManager.getTaskResource(uid, data, resource.getScheme());

        ((FunctionTask) dispatchInfo.getTaskInfo().getTask()).setResource(baseResource);
        log.info("handle invoke super method logic, executionId:{} taskName:{}", dispatchInfo.getExecutionId(), dispatchInfo.getTaskInfo().getName());
        return resourceProtocolDispatcher.handle(resource, dispatchInfo);
    }

    @Override
    public String getName() {
        return "resourceRef";
    }
}
