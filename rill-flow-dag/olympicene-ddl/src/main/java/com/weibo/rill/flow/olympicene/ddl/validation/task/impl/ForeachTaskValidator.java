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
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.olympicene.core.model.task.ForeachTask;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;
import com.weibo.rill.flow.olympicene.ddl.validation.TaskValidator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ForeachTaskValidator implements TaskValidator<ForeachTask> {
    @Override
    public boolean match(BaseTask task) {
        return task instanceof ForeachTask;
    }

    @Override
    public void validate(BaseTask task, Map<String, BaseResource> resourceMap) {
        ForeachTask foreachTask = (ForeachTask) task;
        assertTask(foreachTask, t -> "foreach".equals(t.getCategory())
                , DDLErrorCode.FOREACH_TASK_INVALID.getCode(), String.format(DDLErrorCode.FOREACH_TASK_INVALID.getMessage(), task.getName(), "category is invalid"));
        assertTask(foreachTask, t -> t.getIterationMapping() != null
                , DDLErrorCode.FOREACH_TASK_INVALID.getCode(), String.format(DDLErrorCode.FOREACH_TASK_INVALID.getMessage(), task.getName(), "iterationMapping is null"));
        assertTask(foreachTask, t -> !StringUtils.isEmpty(t.getIterationMapping().getCollection())
                , DDLErrorCode.FOREACH_TASK_INVALID.getCode(), String.format(DDLErrorCode.FOREACH_TASK_INVALID.getMessage(), task.getName(), "iterationMapping collection is empty"));
        assertTask(foreachTask, t -> CollectionUtils.isNotEmpty(t.getTasks())
                , DDLErrorCode.FOREACH_TASK_INVALID.getCode(), String.format(DDLErrorCode.FOREACH_TASK_INVALID.getMessage(), task.getName(), "tasks is empty"));
    }

}
