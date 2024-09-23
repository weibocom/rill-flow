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

import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.model.task.AnswerTask;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class AnswerTaskRunner extends AbstractTaskRunner {
    private final DAGDispatcher answerTaskDispatcher;

    public AnswerTaskRunner(DAGDispatcher answerTaskDispatcher, InputOutputMapping inputOutputMapping, DAGInfoStorage dagInfoStorage,
                            DAGContextStorage dagContextStorage, DAGStorageProcedure dagStorageProcedure,
                            SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
        this.answerTaskDispatcher = answerTaskDispatcher;
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.ANSWER;
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        DispatchInfo dispatchInfo = DispatchInfo.builder()
                .taskInfo(taskInfo)
                .input(input)
                .executionId(executionId)
                .build();
        boolean tolerance = Optional.ofNullable(taskInfo.getTask()).map(BaseTask::isTolerance).orElse(false);
        TaskStatus retStatus = null;
        try {
            AnswerTask answerTask = (AnswerTask) taskInfo.getTask();
            if (answerTask == null || StringUtils.isEmpty(answerTask.getExpression())) {
                TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().msg("answer task expression empty").build();
                taskInfo.updateInvokeMsg(taskInvokeMsg);
                retStatus = TaskStatus.SKIPPED;
                return ExecutionResult.builder().taskStatus(retStatus).build();
            }
            updateTaskInvokeStartTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.RUNNING);
            dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
            String dispatchResult = answerTaskDispatcher.dispatch(dispatchInfo);
            log.info("dispatch answer task succeed, execution_id: {}, task_name: {}, result: {}", executionId, taskInfo.getName(), dispatchResult);
            retStatus = TaskStatus.SUCCEED;
            return ExecutionResult.builder().taskStatus(retStatus).build();
        } catch (Exception e) {
            log.warn("dispatch answer task failed, execution_id: {}, task_name: {}", executionId, taskInfo.getName(), e);
            retStatus = tolerance? TaskStatus.SKIPPED: TaskStatus.FAILED;
            return ExecutionResult.builder().taskStatus(retStatus).build();
        } finally {
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(retStatus);
            dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
        }
    }

    @Override
    public String getIcon() {
        return "ant-design:audio-outlined";
    }
}
