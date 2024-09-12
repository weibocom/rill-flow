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
import com.weibo.rill.flow.olympicene.core.model.task.*;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException;
import com.weibo.rill.flow.olympicene.ddl.validation.TaskValidator;
import com.weibo.rill.flow.interfaces.model.task.*;

import java.util.List;
import java.util.Map;

public class NotSupportedTaskValidator implements TaskValidator<BaseTask> {
    private final List<Class<?>> supportedTasks = List.of(FunctionTask.class, SuspenseTask.class, PassTask.class,
            ReturnTask.class, ForeachTask.class, ChoiceTask.class, SwitchTask.class, AnswerTask.class);

    @Override
    public void validate(BaseTask task, Map<String, BaseResource> resourceMap) {
        if (!supportedTasks.contains(task.getClass())) {
            throw new ValidationException(DDLErrorCode.NOT_SUPPORTED_TASK);
        }
    }

    @Override
    public boolean match(BaseTask task) {
        return true;
    }
}
