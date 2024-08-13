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

import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.ReturnTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.utils.ConditionsUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
public class ReturnTaskRunner extends AbstractTaskRunner {
    public ReturnTaskRunner(InputOutputMapping inputOutputMapping,
                            DAGInfoStorage dagInfoStorage,
                            DAGContextStorage dagContextStorage,
                            DAGStorageProcedure dagStorageProcedure,
                            SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.RETURN;
    }

    @Override
    public String getIcon() {
        return "ant-design:filter-outlined";
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("return task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        ReturnTask returnTask = (ReturnTask) taskInfo.getTask();
        boolean needReturn = ConditionsUtil.conditionsAllMatch(returnTask.getConditions(), input, "input");
        log.info("needReturn conditions {}", needReturn);

        if (!needReturn) {
            taskInfo.setTaskStatus(TaskStatus.SKIPPED);
        } else {
            // 标记所有后继节点为需要跳过
            taskInfo.setSkipNextTaskNames(taskInfo.getNext().stream().map(TaskInfo::getName).collect(Collectors.toSet()));
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
        }
        updateTaskInvokeEndTime(taskInfo);

        dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
        log.info("run return task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
    }
}
