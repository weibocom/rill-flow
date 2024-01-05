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

package com.weibo.rill.flow.configuration;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.interfaces.model.task.InvokeTimeInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.Olympicene;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.notify.NotifyType;
import com.weibo.rill.flow.olympicene.traversal.runners.TimeCheckRunner;
import com.weibo.rill.flow.service.dispatcher.FlowProtocolDispatcher;
import com.weibo.rill.flow.service.statistic.BusinessTimeChecker;
import com.weibo.rill.flow.service.statistic.SystemMonitorStatistic;
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;


@Slf4j
@Configuration
public class BeanPostConfig {
    private static final String EXECUTION_ID = "executionId";

    @Autowired
    private SystemMonitorStatistic systemMonitorStatistic;
    @Autowired
    private TenantTaskStatistic tenantTaskStatistic;
    @Autowired
    private SwitcherManager switcherManagerImpl;

    @Autowired
    public void setDispatcherProperty(
            @Autowired @Qualifier("olympicene") Olympicene olympicene,
            @Autowired FlowProtocolDispatcher flowDispatcher) {
        flowDispatcher.setOlympicene(olympicene);
    }

    @Autowired
    public void setBusinessTimeCheckProperty(
            @Autowired BusinessTimeChecker businessTimeChecker,
            @Autowired TimeCheckRunner timeCheckRunner) {
        businessTimeChecker.setTimeCheckRunner(timeCheckRunner);
    }

    @Autowired
    public void setNotifyPlugins(
            @Autowired @Qualifier("statisticExecutor") ExecutorService statisticExecutor) {
        BiConsumer<Runnable, Map<String, Object>> statisticLogPlugin = (nextActions, params) -> {
            long startTime = System.currentTimeMillis();

            try {
                nextActions.run();
            } finally {
                asyncExecution(statisticExecutor, () -> {
                    long executionCost = System.currentTimeMillis() - startTime;
                    String executionId = getParam(params, EXECUTION_ID, String.class);
                    NotifyType notifyType = getParam(params, "notifyType", NotifyType.class);
                    NotifyInfo notifyInfo = getParam(params, "notifyInfo", NotifyInfo.class);

                    if (StringUtils.isBlank(executionId) || notifyType == null) {
                        log.info("notify statisticLogPlugin skip, executionId:{}, notifyType:{}", executionId, notifyType);
                    } else {
                        systemMonitorStatistic.recordNotify(executionCost, executionId, notifyType);
                        if (notifyType == NotifyType.FINISH) {
                            tenantTaskStatistic.finishNotifyCount(executionId, notifyInfo);
                        } else if (notifyType == NotifyType.SUBMIT) {
                            tenantTaskStatistic.dagSubmitCount(executionId);
                        }
                    }
                });
            }
        };

        SystemConfig.NOTIFY_CUSTOMIZED_PLUGINS.addAll(Lists.newArrayList(statisticLogPlugin));
    }

    @Autowired
    public void setTraversalPlugins(
            @Autowired @Qualifier("statisticExecutor") ExecutorService statisticExecutor) {
        BiConsumer<Runnable, Map<String, Object>> statisticLogPlugin = (nextActions, params) -> {
            long startTime = System.currentTimeMillis();

            try {
                nextActions.run();
            } finally {
                asyncExecution(statisticExecutor, () -> {
                    boolean independentSwitcher = switcherManagerImpl.getSwitcherState("ENABLE_FLOW_CONCURRENT_TASK_INDEPENDENT_CONTEXT");
                    if (independentSwitcher != ContextHelper.getInstance().isIndependentContext()) {
                        ContextHelper.getInstance().setIndependentContext(independentSwitcher);
                        log.info("independent context switcher value change to {}", ContextHelper.getInstance().isIndependentContext());
                    }

                    long executionCost = System.currentTimeMillis() - startTime;
                    String executionId = getParam(params, EXECUTION_ID, String.class);

                    if (StringUtils.isBlank(executionId)) {
                        log.info("traversal statisticLogPlugin skip due to executionId empty");
                    } else {
                        systemMonitorStatistic.recordTraversal(executionCost, executionId);
                    }
                });
            }
        };

        SystemConfig.TRAVERSAL_CUSTOMIZED_PLUGINS.addAll(Lists.newArrayList(statisticLogPlugin));
    }

    @Autowired
    public void setOperationsPlugins(
            @Autowired @Qualifier("statisticExecutor") ExecutorService statisticExecutor) {
        setCustomizedTaskRunPlugins(statisticExecutor);
        setCustomizedDAGFinishPlugins(statisticExecutor);
        setTaskFinishPlugins(statisticExecutor);
    }

    private void setCustomizedDAGFinishPlugins(ExecutorService statisticExecutor) {
        BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult> statisticLogPlugin = (nextActions, params) -> {
            long startTime = System.currentTimeMillis();

            try {
                return nextActions.get();
            } finally {
                asyncExecution(statisticExecutor, () -> {
                    long executionCost = System.currentTimeMillis() - startTime;
                    String executionId = getParam(params, EXECUTION_ID, String.class);
                    DAGStatus dagStatus = getParam(params, "dagStatus", DAGStatus.class);
                    DAGInfo dagInfo = getParam(params, "dagInfo", DAGInfo.class);

                    if (StringUtils.isBlank(executionId)) {
                        log.info("taskCollect statisticLogPlugin skip due to executionId empty");
                    } else {
                        systemMonitorStatistic.recordDAGFinish(executionId, executionCost, dagStatus, dagInfo);
                        tenantTaskStatistic.dagFinishCount(executionId, dagInfo);
                    }
                });
            }
        };

        SystemConfig.DAG_FINISH_CUSTOMIZED_PLUGINS.addAll(Lists.newArrayList(statisticLogPlugin));
    }

    private void setTaskFinishPlugins(ExecutorService statisticExecutor) {
        BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult> statisticLogPlugin = (nextActions, params) -> {
            ExecutionResult executionResult = null;
            try {
                executionResult = nextActions.get();
            } finally {
                Optional<TaskInfo> taskInfoOpt = Optional.ofNullable(executionResult).map(ExecutionResult::getTaskInfo);
                asyncExecutionWhenMeetCondition(switcherManagerImpl.getSwitcherState("ENABLE_RECORD_COMPLIANCE_WHEN_TASK_FINISHED"), statisticExecutor, () -> {
                    String executionId = getParam(params, EXECUTION_ID, String.class);
                    Optional<InvokeTimeInfo> invokeTimeInfoOpt = taskInfoOpt.map(TaskInfo::getTaskInvokeMsg)
                            .map(TaskInvokeMsg::getInvokeTimeInfos)
                            .filter(CollectionUtils::isNotEmpty)
                            .map(it -> it.get(it.size() - 1));

                    Long startTime = invokeTimeInfoOpt.map(InvokeTimeInfo::getStartTimeInMillisecond).orElse(null);
                    Long endTime = invokeTimeInfoOpt.map(InvokeTimeInfo::getEndTimeInMillisecond).orElse(null);
                    Long expectedCost = invokeTimeInfoOpt.map(InvokeTimeInfo::getExpectedCostInMillisecond).orElse(null);

                    if (taskInfoOpt.isPresent() && ObjectUtils.allNotNull(startTime, endTime, expectedCost)) {
                        if (StringUtils.isBlank(executionId)) {
                            log.info("taskRun compliance statisticLogPlugin skip, executionId:{}.", executionId);
                        } else {
                            long actualCost = endTime - startTime;
                            long timeoutPartPercentage = (actualCost - expectedCost) * 100 / expectedCost;

                            systemMonitorStatistic.recordTaskCompliance(executionId,
                                    taskInfoOpt.get(),
                                    actualCost < expectedCost,
                                    timeoutPartPercentage);
                        }
                    }
                });
            }
            return executionResult;
        };

        SystemConfig.TASK_FINISH_CUSTOMIZED_PLUGINS.addAll(Lists.newArrayList(statisticLogPlugin));
    }

    private void setCustomizedTaskRunPlugins(ExecutorService statisticExecutor) {
        BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult> statisticLogPlugin = (nextActions, params) -> {
            long startTime = System.currentTimeMillis();

            try {
                return nextActions.get();
            } finally {
                asyncExecution(statisticExecutor, () -> {
                    long executionCost = System.currentTimeMillis() - startTime;
                    String executionId = getParam(params, EXECUTION_ID, String.class);
                    TaskInfo taskInfo = getParam(params, "taskInfo", TaskInfo.class);

                    if (StringUtils.isBlank(executionId) || taskInfo == null) {
                        log.info("taskRun statisticLogPlugin skip, executionId:{}, taskInfo empty:{}", executionId, taskInfo == null);
                    } else {
                        systemMonitorStatistic.recordTaskRun(executionCost, executionId, taskInfo);
                        tenantTaskStatistic.recordTaskRun(executionCost, executionId, taskInfo);
                    }
                });
            }
        };

        SystemConfig.TASK_RUN_CUSTOMIZED_PLUGINS.addAll(Lists.newArrayList(statisticLogPlugin));
    }

    @SuppressWarnings("unchecked")
    private <T> T getParam(Map<String, Object> params, String paramName, Class<T> paramType) {
         return (T) Optional.ofNullable(params)
                .map(it -> it.get(paramName))
                .filter(it -> paramType.isAssignableFrom(it.getClass()))
                .orElse(null);
    }

    private void asyncExecution(ExecutorService executorService, Runnable actions) {
        try {
            executorService.execute(actions);
        } catch (Exception e) {
            log.warn("asyncExecution execute fails, ", e);
        }
    }

    private void asyncExecutionWhenMeetCondition(boolean condition, ExecutorService executorService, Runnable actions) {
        if (!condition) {
            return;
        }

        asyncExecution(executorService, actions);
    }
}
