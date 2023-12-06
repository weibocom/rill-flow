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

package com.weibo.rill.flow.service.util;

import com.weibo.rill.flow.common.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xinyu55
 */
@Slf4j
public class PrometheusActions {

    public static final String REACHED = "REACHED";
    public static final String NOT_REACHED = "NOT_REACHED";

    public static final String METER_PREFIX = "flow_meter_";

    private static final String DAG = "dag_";

    private static final String DAG_COMPLETE = "dag_complete_";

    private static final String SUCCESS = "SUCCESS";

    private static final String FAIL = "FAIL";

    private static final String DAG_TIME_FORMAT = "cost_%s";
    private static final String TASK_TIME_FORMAT = "%s_%s";
    private static final String DAG_COMPLETE_FORMAT = "flow_%s";
    private static final String TASK_COMPLETE_FORMAT = "%s_%s";
    private static final String TASK_COMPLIANCE_FORMAT = "compliance_%s_%s_%s";
    private static final String TASK_COMPLIANCE_PERCENTAGE_FORMAT = "compliance_percentage_%s_%s_%s";
    private static final String EXECUTION_STATUS_FORMAT = "sta_rate_%s" + ReservedConstant.EXECUTION_ID_CONNECTOR + "%s";
    private static final String TASK_CODE_FORMAT = "code_%s_%s_%s";
    private static final String TINY_DAG_FORMAT = "tiny_dag_%s";
    private static final String HTTP_EXECUTION_FORMAT = "%s_%s";

    public static void recordDagTotalExecutionTime(long executionTime, String serviceId) {
        String name = String.format(DAG_TIME_FORMAT, serviceId);
        PrometheusUtil.statisticsTotalTime(METER_PREFIX + DAG + name, executionTime);
    }

    public static void recordTaskTotalExecutionTime(long executionCost, String action, String serviceId) {
        String name = String.format(TASK_TIME_FORMAT, action, serviceId);
        PrometheusUtil.statisticsTotalTime(METER_PREFIX + DAG + name, executionCost);
    }

    public static void recordTaskCompliance(String serviceId, String category, String taskName, boolean reached, long percentage) {
        String countName = String.format(TASK_COMPLIANCE_FORMAT, category, serviceId, taskName);
        String percentageName = String.format(TASK_COMPLIANCE_PERCENTAGE_FORMAT, category, serviceId, taskName);
        PrometheusUtil.statisticsTotalTime(METER_PREFIX + DAG + percentageName, percentage);
        PrometheusUtil.count(METER_PREFIX + DAG + countName + "_" + (reached ? REACHED : NOT_REACHED));
    }

    public static void recordExecutionStatus(DAGStatus dagStatus, String serviceId, Long count) {
        String name = executionStatusKey(serviceId, dagStatus);
        PrometheusUtil.count(METER_PREFIX + DAG + name, count.intValue());
    }

    private static String executionStatusKey(String serviceId, DAGStatus dagStatus) {
        return String.format(EXECUTION_STATUS_FORMAT, serviceId, dagStatus.getValue());
    }

    public static void recordDAGComplete(String executionId, long executionCost) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(DAG_COMPLETE_FORMAT, serviceId);
            PrometheusUtil.statisticsTotalTime(METER_PREFIX + DAG_COMPLETE + name, executionCost);
        } catch (Exception e) {
            log.warn("PrometheusActions recordDAGComplete fails, current executionId:{}, errorMsg:{}", executionId, e.getMessage());
        }
    }

    public static void recordTaskComplete(String executionId, String taskCategory, long executionCost) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(TASK_COMPLETE_FORMAT, taskCategory, serviceId);
            PrometheusUtil.statisticsTotalTime(METER_PREFIX + DAG_COMPLETE + name, executionCost);
        } catch (Exception e) {
            log.warn("PrometheusActions recordTaskComplete fails, executionId:{}, taskCategory:{}, errorMsg:{}", executionId, taskCategory, e.getMessage());
        }
    }

    public static void recordTaskCode(String executionId, String code, String suffix) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(TASK_CODE_FORMAT, serviceId, code, suffix);
            PrometheusUtil.count(METER_PREFIX + DAG_COMPLETE + name);
        } catch (Exception e) {
            log.warn("PrometheusActions recordTaskCode fails, executionId:{}, suffix:{}, errorMsg:{}", executionId, suffix, e.getMessage());
        }
    }

    public static void recordHttpExecution(String url, String id, boolean is200, long costTimeMillis) {
        try {
            int index = url.indexOf('?');
            String urlPath = index < 0 ? url : url.substring(0, index);
            String tagValue = is200 ? SUCCESS : FAIL;
            String name = String.format(HTTP_EXECUTION_FORMAT, urlPath, ExecutionIdUtil.getServiceId(id));
            PrometheusUtil.statisticsTotalTime(name, costTimeMillis, "status", tagValue);
        } catch (Exception e) {
            log.warn("PrometheusActions recordHttpExecution fails, url:{}, id:{}, is200:{}, costTimeMillis:{}, errorMsg:{}", url, id, is200, costTimeMillis, e.getMessage());
        }
    }

    public static void recordTinyDAGSubmit(String executionId) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(TINY_DAG_FORMAT, serviceId);
            PrometheusUtil.count(METER_PREFIX + DAG + name);
        } catch (Exception e) {
            log.warn("PrometheusActions recordTinyDAGSubmit fails, executionId:{}, errorMsg:{}", executionId, e.getMessage());
        }
    }

    private PrometheusActions() {

    }
}
