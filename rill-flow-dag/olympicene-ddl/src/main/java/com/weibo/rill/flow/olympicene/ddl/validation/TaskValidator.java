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

package com.weibo.rill.flow.olympicene.ddl.validation;

import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException;

import java.util.Map;
import java.util.function.Predicate;

public interface TaskValidator<T extends BaseTask > {
    boolean match(BaseTask task);
    void validate(BaseTask task, Map<String, BaseResource> resourceMap);
    default void assertTask(T task, Predicate<T> predicate, int code, String message) {
        if (!predicate.test(task)) {
            throw new ValidationException(code, message);
        }
    }
}
