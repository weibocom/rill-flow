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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PassTaskRunner extends AbstractTaskRunner {

    public PassTaskRunner(InputOutputMapping inputOutputMapping,
                          DAGContextStorage dagContextStorage,
                          DAGInfoStorage dagInfoStorage,
                          DAGStorageProcedure dagStorageProcedure,
                          SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.PASS;
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("pass task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        if (CollectionUtils.isNotEmpty(taskInfo.getTask().getOutputMappings())) {
            Map<String, Object> context = ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);
            outputMappings(context, new HashMap<>(), new HashMap<>(), taskInfo.getTask().getOutputMappings());
            saveContext(executionId, context, Sets.newHashSet(taskInfo));
        }

        taskInfo.setTaskStatus(TaskStatus.SUCCEED);
        updateTaskInvokeEndTime(taskInfo);
        dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
        
        log.info("run pass task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
    }
}
