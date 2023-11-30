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
import com.weibo.rill.flow.common.model.ProfileType;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class ProfileActions {
    private static final ProfileType HTTP_200 = new ProfileType("dag_http_2xx");
    private static final ProfileType HTTP_400_500 = new ProfileType("dag_http_4xx_5xx");
    private static final ProfileType DAG = new ProfileType("dag");
    private static final ProfileType DAG_TASK_COMPLIANCE = new ProfileType("dag", 0, 25, 50, 100, 50);
    private static final ProfileType DAG_COMPLETE = new ProfileType("dag_complete");
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
    public static final String REACHED = "REACHED";
    public static final String NOT_REACHED = "NOTREACHED";

    public static void recordDagTotalExecutionTime(long executionTime, String serviceId) {
        String name = String.format(DAG_TIME_FORMAT, serviceId);
        ProfileUtil.count(DAG, name, System.currentTimeMillis(), (int) executionTime);
    }

    public static void recordTaskTotalExecutionTime(long executionCost, String action, String serviceId) {
        String name = String.format(TASK_TIME_FORMAT, action, serviceId);
        ProfileUtil.accessStatistic(DAG, name, System.currentTimeMillis(), executionCost);
    }

    public static void recordTaskCompliance(String serviceId, String category, String taskName, boolean reached, long percentage) {
        String countName = String.format(TASK_COMPLIANCE_FORMAT, category, serviceId, taskName);
        String percentageName = String.format(TASK_COMPLIANCE_PERCENTAGE_FORMAT, category, serviceId, taskName);
        ProfileUtil.accessStatistic(DAG_TASK_COMPLIANCE, percentageName, System.currentTimeMillis(), percentage);
        ProfileUtil.count(DAG_TASK_COMPLIANCE, countName + "_" + (reached ? REACHED : NOT_REACHED), System.currentTimeMillis(), 1);
    }

    public static void recordExecutionStatus(DAGStatus dagStatus, String serviceId, Long count) {
        int incrNum = Optional.ofNullable(count).orElse(0L).intValue();
        String name = executionStatusKey(serviceId, dagStatus);
        ProfileUtil.count(DAG, name, System.currentTimeMillis(), incrNum);
    }

    private static String executionStatusKey(String serviceId, DAGStatus dagStatus) {
        return String.format(EXECUTION_STATUS_FORMAT, serviceId, dagStatus.getValue());
    }

    public static void recordDAGComplete(String executionId, long executionCost) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(DAG_COMPLETE_FORMAT, serviceId);
            ProfileUtil.accessStatistic(DAG_COMPLETE, name, System.currentTimeMillis(), executionCost);
        } catch (Exception e) {
            log.warn("recordDAGComplete fails, current executionId:{}, errorMsg:{}", executionId, e.getMessage());
        }
    }

    public static void recordTaskComplete(String executionId, String taskCategory, long executionCost) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(TASK_COMPLETE_FORMAT, taskCategory, serviceId);
            ProfileUtil.accessStatistic(DAG_COMPLETE, name, System.currentTimeMillis(), executionCost);
        } catch (Exception e) {
            log.warn("recordTaskComplete fails, executionId:{}, taskCategory:{}, errorMsg:{}", executionId, taskCategory, e.getMessage());
        }
    }

    public static void recordTaskCode(String executionId, String code, String suffix) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(TASK_CODE_FORMAT, serviceId, code, suffix);
            ProfileUtil.count(DAG_COMPLETE, name, System.currentTimeMillis(), 1);
        } catch (Exception e) {
            log.warn("recordTaskCode fails, executionId:{}, suffix:{}, errorMsg:{}", executionId, suffix, e.getMessage());
        }
    }

    public static void recordHttpExecution(String url, String id, boolean is200, long costTimeMillis) {
        try {
            int index = url.indexOf('?');
            String urlPath = index < 0 ? url : url.substring(0, index);
            ProfileType type = is200 ? HTTP_200 : HTTP_400_500;
            String name = String.format(HTTP_EXECUTION_FORMAT, urlPath, ExecutionIdUtil.getServiceId(id));
            ProfileUtil.accessStatistic(type, name, System.currentTimeMillis(), costTimeMillis);
        } catch (Exception e) {
            log.warn("recordHttpExecution fails, url:{}, id:{}, is200:{}, costTimeMillis:{}, errorMsg:{}",
                    url, id, is200, costTimeMillis, e.getMessage());
        }
    }

    public static void recordTinyDAGSubmit(String executionId) {
        try {
            String serviceId = ExecutionIdUtil.getServiceId(executionId);
            String name = String.format(TINY_DAG_FORMAT, serviceId);
            ProfileUtil.count(DAG, name, System.currentTimeMillis(), 1);
        } catch (Exception e) {
            log.warn("recordTinyDAGSubmit fails, executionId:{}, errorMsg:{}", executionId, e.getMessage());
        }
    }

    private ProfileActions() {

    }
}
