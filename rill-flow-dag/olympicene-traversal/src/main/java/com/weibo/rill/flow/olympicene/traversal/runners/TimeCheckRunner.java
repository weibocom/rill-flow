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

import com.google.common.collect.Lists;
import com.weibo.rill.flow.interfaces.model.strategy.Timeline;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.lock.LockerKey;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.traversal.DAGOperations;
import com.weibo.rill.flow.olympicene.traversal.DAGOperationsInterface;
import com.weibo.rill.flow.olympicene.traversal.checker.TimeCheckMember;
import com.weibo.rill.flow.olympicene.traversal.checker.TimeChecker;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class TimeCheckRunner {
    private final TimeChecker timeChecker;
    private final DAGStorageProcedure dagStorageProcedure;
    private final DAGInfoStorage dagInfoStorage;
    private final DAGContextStorage dagContextStorage;
    @Setter
    private DAGOperationsInterface dagOperations;

    public TimeCheckRunner(TimeChecker timeChecker, DAGInfoStorage dagInfoStorage, DAGContextStorage dagContextStorage,
                           DAGStorageProcedure dagStorageProcedure) {
        this.timeChecker = timeChecker;
        this.dagInfoStorage = dagInfoStorage;
        this.dagContextStorage = dagContextStorage;
        this.dagStorageProcedure = dagStorageProcedure;
    }

    public void handleTimeCheck(String timeCheckMember) {
        try {
            log.info("handleTimeCheck start timeCheckMember:{}", timeCheckMember);
            byte[] memberByte = timeCheckMember.getBytes(StandardCharsets.UTF_8);
            TimeCheckMember member = DAGTraversalSerializer.deserialize(memberByte, TimeCheckMember.class);

            TimeCheckMember.CheckMemberType type = member.getCheckMemberType();
            String executionId = member.getExecutionId();

            switch (type) {
                case DAG_TIMEOUT_CHECK:
                    DAGInvokeMsg dagInvokeMsg = DAGInvokeMsg.builder().msg("timeout").build();
                    dagStorageProcedure.lockAndRun(
                            LockerKey.buildDagInfoLockName(executionId),
                            () -> dagOperations.finishDAG(executionId, null, DAGStatus.FAILED, dagInvokeMsg));
                    break;
                case TASK_TIMEOUT_CHECK:
                    NotifyInfo notifyInfo = NotifyInfo.builder()
                            .taskInfoName(member.getTaskInfoName())
                            .taskStatus(TaskStatus.FAILED)
                            .taskInvokeMsg(TaskInvokeMsg.builder().msg("timeout").build())
                            .build();
                    dagOperations.finishTaskSync(executionId, member.getTaskCategory(), notifyInfo, new HashMap<>());
                    break;
                case TASK_WAIT_CHECK:
                    Runnable operations = () -> {
                        TaskInfo taskInfo = dagInfoStorage.getBasicTaskInfo(executionId, member.getTaskInfoName());
                        Map<String, Object> context = ContextHelper.getInstance().getContext(dagContextStorage, executionId, taskInfo);
                        dagOperations.runTasks(executionId, Lists.newArrayList(Pair.of(taskInfo, context)));
                    };
                    DAGOperations.OPERATE_WITH_RETRY.accept(operations, SystemConfig.getTimerRetryTimes());
                    break;
                default:
                    log.warn("handleTimeCheck time check type nonsupport, type:{}", type);
            }
        } catch (Exception e) {
            log.warn("handleTimeCheck fails, timeCheckMember:{}", timeCheckMember, e);
        }
    }

    public void addDAGToTimeoutCheck(String executionId, long timeoutSeconds) {
        try {
            log.info("addDAGToTimeoutCheck start execute executionId:{} timeoutSeconds:{}", executionId, timeoutSeconds);
            long timeout = System.currentTimeMillis() + timeoutSeconds * 1000;
            timeChecker.addMemberToCheckPool(executionId, buildDAGTimeoutCheckMember(executionId), timeout);
        } catch (Exception e) {
            log.warn("addDAGToTimeoutCheck fails, executionId:{}", executionId, e);
        }
    }

    private String buildDAGTimeoutCheckMember(String executionId) {
        TimeCheckMember member = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.DAG_TIMEOUT_CHECK)
                .executionId(executionId)
                .build();
        return DAGTraversalSerializer.serializeToString(member);
    }

    public void remDAGFromTimeoutCheck(String executionId, DAG dag) {
        try {
            Optional.ofNullable(dag)
                    .map(DAG::getTimeline)
                    .map(Timeline::getTimeoutInSeconds)
                    .filter(StringUtils::isNotBlank)
                    .ifPresent(timeoutSeconds -> {
                        log.info("remDAGFromTimeoutCheck start executionId:{}", executionId);
                        String member = buildDAGTimeoutCheckMember(executionId);
                        timeChecker.remMemberFromCheckPool(executionId, member);
                    });
        } catch (Exception e) {
            log.warn("remDAGFromTimeoutCheck fails, executionId:{}", executionId, e);
        }
    }

    public void addTaskToTimeoutCheck(String executionId, TaskInfo taskInfo, long timeoutSeconds) {
        try {
            log.info("addTaskToTimeoutCheck start execute executionId:{} taskInfoName:{} timeoutSeconds:{}",
                    executionId, taskInfo.getName(), timeoutSeconds);
            long timeout = System.currentTimeMillis() + timeoutSeconds * 1000;
            String member = buildTaskTimeoutCheckMember(executionId, taskInfo);
            timeChecker.addMemberToCheckPool(executionId, member, timeout);
        } catch (Exception e) {
            log.warn("addTaskToTimeoutCheck fails, executionId:{}, taskInfoName:{}",
                    executionId, Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null), e);
        }
    }

    private String buildTaskTimeoutCheckMember(String executionId, TaskInfo taskInfo) {
        TimeCheckMember member = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.TASK_TIMEOUT_CHECK)
                .executionId(executionId)
                .taskCategory(taskInfo.getTask().getCategory())
                .taskInfoName(taskInfo.getName()).build();
        return DAGTraversalSerializer.serializeToString(member);
    }

    public void remTaskFromTimeoutCheck(String executionId, TaskInfo taskInfo) {
        try {
            Optional.ofNullable(taskInfo)
                    .map(TaskInfo::getTask)
                    .map(BaseTask::getTimeline)
                    .map(Timeline::getTimeoutInSeconds)
                    .filter(StringUtils::isNotBlank)
                    .ifPresent(timeoutConfig -> {
                        log.info("remTaskFromTimeoutCheck start execute executionId:{} taskInfoName:{}", executionId, taskInfo.getName());
                        String member = buildTaskTimeoutCheckMember(executionId, taskInfo);
                        timeChecker.remMemberFromCheckPool(executionId, member);
                    });
        } catch (Exception e) {
            log.warn("remTaskFromTimeoutCheck fails, executionId:{}, taskInfoName:{}",
                    executionId, Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null), e);
        }
    }

    public void addTaskToWaitCheck(String executionId, TaskInfo taskInfo, int intervalInSeconds) {
        try {
            log.info("addTaskToWaitCheck start execute executionId:{} taskInfoName:{}", executionId, taskInfo.getName());

            TimeCheckMember timeCheckMember = TimeCheckMember.builder()
                    .checkMemberType(TimeCheckMember.CheckMemberType.TASK_WAIT_CHECK)
                    .executionId(executionId)
                    .taskCategory(taskInfo.getTask().getCategory())
                    .taskInfoName(taskInfo.getName()).build();
            String member = DAGTraversalSerializer.serializeToString(timeCheckMember);
            long timeout = System.currentTimeMillis() + intervalInSeconds * 1000L;

            timeChecker.addMemberToCheckPool(executionId, member, timeout);
        } catch (Exception e) {
            log.warn("addTaskToWaitCheck fails, executionId:{}, taskInfoName:{}",
                    executionId, Optional.ofNullable(taskInfo).map(TaskInfo::getName).orElse(null), e);
        }
    }
}
