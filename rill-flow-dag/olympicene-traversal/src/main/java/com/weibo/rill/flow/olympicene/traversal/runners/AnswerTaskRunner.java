package com.weibo.rill.flow.olympicene.traversal.runners;

import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class AnswerTaskRunner extends AbstractTaskRunner {
    public AnswerTaskRunner(InputOutputMapping inputOutputMapping, DAGInfoStorage dagInfoStorage,
                            DAGContextStorage dagContextStorage, DAGStorageProcedure dagStorageProcedure,
                            SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.ANSWER;
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        // TODO
        return null;
    }
}
