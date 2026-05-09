package com.weibo.rill.flow.olympicene.traversal;

import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.Map;

public interface DAGOperationsInterface {
    void finishDAG(String executionId, DAGInfo dagInfo, DAGStatus dagStatus, DAGInvokeMsg dagInvokeMsg);
    void finishTaskAsync(String executionId, String taskCategory, NotifyInfo notifyInfo, Map<String, Object> output);
    void finishTaskSync(String executionId, String taskCategory, NotifyInfo notifyInfo, Map<String, Object> output);
    void runTasks(String executionId, Collection<Pair<TaskInfo, Map<String, Object>>> taskInfoToContexts);
}
