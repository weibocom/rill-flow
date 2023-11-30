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

package com.weibo.rill.flow.service.statistic;

import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;

import java.util.Map;



public interface TenantTaskStatistic {
    void recordTaskRun(long executionTime, String executionId, TaskInfo taskInfo);

    void taskProfileLog(long executionTime, String executionId, String taskInfoName, String taskExecutionType);

    void recordTaskStashProfileLog(long executionTime, String executionId, String taskInfoName, String stashType, boolean isSuccess);

    void recordFlowStashProfileLog(long executionTime, String executionId, String stashType, boolean isSuccess);

    void dagFinishCount(String executionId, DAGInfo dagInfo);

    void dagSubmitCount(String executionId);

    void finishNotifyCount(String executionId, NotifyInfo notifyInfo);

    Map<String, String> getBusinessAggregate(String businessKey);

    Map<String, String> getFlowAggregate(String executionId);

    void setBusinessValue();
}
