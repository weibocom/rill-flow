package com.weibo.rill.flow.olympicene.traversal.runners;

import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
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
        try {
            AnswerTask answerTask = (AnswerTask) taskInfo.getTask();
            if (answerTask == null || StringUtils.isEmpty(answerTask.getExpression())) {
                TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().msg("answer task expression empty").build();
                taskInfo.updateInvokeMsg(taskInvokeMsg);
                updateTaskInvokeEndTime(taskInfo);
                taskInfo.setTaskStatus(TaskStatus.SKIPPED);
                dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
                return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
            }
            updateTaskInvokeStartTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.RUNNING);
            dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
            String dispatchResult = answerTaskDispatcher.dispatch(dispatchInfo);
            log.info("dispatch answer task succeed, execution_id: {}, task_name: {}, result: {}", executionId, taskInfo.getName(), dispatchResult);
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
            dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
            return ExecutionResult.builder().taskStatus(TaskStatus.SUCCEED).build();
        } catch (Exception e) {
            log.warn("dispatch answer task failed, execution_id: {}, task_name: {}", executionId, taskInfo.getName(), e);
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.FAILED);
            dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
            return ExecutionResult.builder().taskStatus(TaskStatus.FAILED).build();
        }
    }
}
