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

package com.weibo.rill.flow.olympicene.ddl.validation.task.impl;

import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;
import com.weibo.rill.flow.olympicene.ddl.validation.TaskValidator;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.function.Predicate;

public class FunctionTaskValidator implements TaskValidator<FunctionTask> {
    @Override
    public boolean match(BaseTask task) {
        return task instanceof FunctionTask;
    }

    @Override
    public void validate(BaseTask task, Map<String, BaseResource> resourceMap) {
        FunctionTask functionTask = (FunctionTask) task;
        assertTask(functionTask, t -> "function".equals(t.getCategory())
                , DDLErrorCode.FUNCTION_TASK_INVALID.getCode(), String.format(DDLErrorCode.FUNCTION_TASK_INVALID.getMessage(), task.getName(), "category is invalid"));
        assertTask(functionTask, t -> t.getPattern() != null
                , DDLErrorCode.FUNCTION_TASK_INVALID.getCode(), String.format(DDLErrorCode.FUNCTION_TASK_INVALID.getMessage(), task.getName(), "pattern can not be null"));
        Predicate<FunctionTask> predicate = it -> {
            if (it.getResource() != null) {
                return true;
            }

            if (StringUtils.isBlank(it.getResourceName()) && StringUtils.isBlank(it.getResourceProtocol())) {
                return false;
            }

            if (StringUtils.isBlank(it.getResourceName())) {
                it.setResourceName(it.getResourceProtocol() + Resource.CONNECTOR);
            }
            String[] values = it.getResourceName().split(ReservedConstant.FUNCTION_TASK_RESOURCE_NAME_SCHEME_CONNECTOR);
            if (values.length == 2) {
                String schemeProtocol = values[0];
                String schemeValue = values[1];
                return !"resource".equals(schemeProtocol) || resourceMap.containsKey(schemeValue);
            }
            return true;
        };
        assertTask(functionTask, predicate, DDLErrorCode.FUNCTION_TASK_INVALID.getCode(),
                String.format(DDLErrorCode.FUNCTION_TASK_INVALID.getMessage(), task.getName(), "resourceName or resource can not be empty"));
    }

}
