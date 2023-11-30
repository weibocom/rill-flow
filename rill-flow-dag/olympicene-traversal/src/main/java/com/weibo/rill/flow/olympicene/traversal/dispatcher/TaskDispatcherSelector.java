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

package com.weibo.rill.flow.olympicene.traversal.dispatcher;

import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class TaskDispatcherSelector implements DAGDispatcher {
    private final Map<String, DAGDispatcher> taskDispatcherMap = new ConcurrentHashMap<>();

    public TaskDispatcherSelector(Map<String, DAGDispatcher> taskDispatcherMap) {
        if (MapUtils.isEmpty(taskDispatcherMap)) {
            return;
        }
        this.taskDispatcherMap.putAll(taskDispatcherMap);
    }

    @Override
    public String dispatch(DispatchInfo taskGroup) {
        String taskCategory = Optional.ofNullable(taskGroup)
                .map(DispatchInfo::getTaskInfo)
                .map(TaskInfo::getTask)
                .map(BaseTask::getCategory)
                .orElse(null);
        if (taskCategory == null) {
            log.warn("handle can not get taskCategory");
            throw new DAGTraversalException(TraversalErrorCode.DAG_NOT_FOUND.getCode(), "handle task fails, cannot find taskCategory");
        }

        DAGDispatcher dispatcher = taskDispatcherMap.get(taskCategory);
        if (dispatcher == null) {
            log.warn("handle task dispatcher not defined");
            throw new DAGTraversalException(TraversalErrorCode.OPERATION_UNSUPPORTED.getCode(), "handle task fails, cannot find dispatcher for task type: " + taskCategory);
        }

        return dispatcher.dispatch(taskGroup);
    }
}
