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

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.traversal.notify.NotifyType;
import com.weibo.rill.flow.common.model.BusinessHeapStatus;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;


public interface SystemMonitorStatistic {
    void recordNotify(long executionCost, String executionId, NotifyType notifyType);

    void recordTraversal(long executionCost, String executionId);

    void recordTaskRun(long executionCost, String executionId, TaskInfo taskInfo);

    void recordTaskCompliance(String executionId, TaskInfo taskInfo, boolean reached, long percentage);

    void recordDAGFinish(String executionId, long executionCost, DAGStatus dagStatus, DAGInfo dagInfo);

    void logExecutionStatus();

    JSONObject businessHeapMonitor(List<String> serviceIds, Integer startTimeOffset, Integer endTimeOffset);

    Map<String, Object> getExecutionCountByStatus(BusinessHeapStatus businessHeapStatus, DAGStatus dagStatus);

    Map<String, Object> getExecutionCountByCode(BusinessHeapStatus businessHeapStatus, String code);

    List<Pair<String, String>> getExecutionIdsByStatus(String serviceId, DAGStatus dagStatus, Long cursor, Integer offset, Integer count);

    List<Pair<String, String>> getExecutionIdsByStatus(String serviceId, DAGStatus dagStatus, Long cursor);

    List<Pair<String, String>> getExecutionIdsByCode(String serviceId, String code, Long cursor, Integer offset, Integer count);

    List<Pair<String, String>> getExecutionIds(String key, Long cursor, Integer offset, Integer count);

    BusinessHeapStatus calculateTimePeriod(String serviceId, Integer startTimeOffset, Integer endTimeOffset);
}
