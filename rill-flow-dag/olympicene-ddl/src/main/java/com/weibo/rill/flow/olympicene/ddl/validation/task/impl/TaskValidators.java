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

import com.google.common.collect.Lists;
import com.weibo.rill.flow.olympicene.ddl.validation.TaskValidator;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

public class TaskValidators {

    public static TaskValidators ALL_TASK_VALIDATORS = new TaskValidators(Lists.newArrayList(
            new NotSupportedTaskValidator(),
            new BaseTaskValidator(),
            new ChoiceTaskValidator(),
            new ForeachTaskValidator(),
            new FunctionTaskValidator()));

    public static List<TaskValidator<?>> allTaskValidations() {
        return ALL_TASK_VALIDATORS.getValidators();
    }

    @Getter
    private final List<TaskValidator<?>> validators = Lists.newArrayList();

    private TaskValidators() {
        // do nothing
    }

    private TaskValidators(List<TaskValidator<?>> validators) {
        Optional.ofNullable(validators).ifPresent(this.validators::addAll);
    }
}
