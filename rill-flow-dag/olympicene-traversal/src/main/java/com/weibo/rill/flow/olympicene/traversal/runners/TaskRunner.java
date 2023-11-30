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

package com.weibo.rill.flow.olympicene.traversal.runners;

import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;

import java.util.List;
import java.util.Map;

public interface TaskRunner {

    void inputMappings(Map<String, Object> context,
                       Map<String, Object> input,
                       Map<String, Object> output,
                       List<Mapping> rules);

    ExecutionResult run(String executionId, TaskInfo taskInfo, Map<String, Object> context);

    ExecutionResult finish(String executionId, NotifyInfo notifyInfo, Map<String, Object> output);

    void outputMappings(Map<String, Object> context,
                        Map<String, Object> input,
                        Map<String, Object> output,
                        List<Mapping> rules);
}
