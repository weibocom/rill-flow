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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.helper.TaskInfoMaker;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.mapping.IterationMapping;
import com.weibo.rill.flow.olympicene.core.model.strategy.Synchronization;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.ForeachTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.helper.Stasher;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPath;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class ForeachTaskRunner extends AbstractTaskRunner {
    private final JSONPath jsonPath;
    @Setter
    private Stasher stasher;

    public ForeachTaskRunner(InputOutputMapping inputOutputMapping,
                             JSONPath jsonPath,
                             DAGContextStorage dagContextStorage,
                             DAGInfoStorage dagInfoStorage,
                             DAGStorageProcedure dagStorageProcedure,
                             SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
        this.jsonPath = jsonPath;
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.FOREACH;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("foreach task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        ForeachTask foreachTask = (ForeachTask) taskInfo.getTask();
        IterationMapping iterationMapping = foreachTask.getIterationMapping();

        Collection<Object> collection = (Collection<Object>) jsonPath.getValue(ImmutableMap.of("input", input), iterationMapping.getCollection());
        if (CollectionUtils.isEmpty(collection) || CollectionUtils.isEmpty(foreachTask.getTasks())) {
            TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().msg("loop collection or subTasks empty").build();
            taskInfo.updateInvokeMsg(taskInvokeMsg);
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
            dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
        }

        log.info("foreach group size:{} executionId:{}, taskInfoName:{}", collection.size(), executionId, taskInfo.getName());
        int maxConcurrentGroups = maxGroupsToRun(executionId, taskInfo, input);
        Map<String, TaskStatus> indexToStatus = Maps.newConcurrentMap();
        taskInfo.setSubGroupIndexToStatus(indexToStatus);
        Map<String, Boolean> indexToKey = Maps.newConcurrentMap();
        taskInfo.setSubGroupKeyJudgementMapping(indexToKey);
        taskInfo.setTaskStatus(TaskStatus.RUNNING);
        taskInfo.setChildren(Optional.ofNullable(taskInfo.getChildren()).orElse(Maps.newConcurrentMap()));
        jsonPath.delete(ImmutableMap.of("input", input), iterationMapping.getCollection());

        AtomicInteger index = new AtomicInteger(0);
        Map<String, Object> contextToUpdate = Maps.newHashMap();
        List<Pair<Set<TaskInfo>, Map<String, Object>>> readyToRun = Lists.newArrayList();
        collection.forEach(item -> {
            int groupIndex = index.getAndIncrement();
            Map<String, TaskInfo> taskInfoMap = TaskInfoMaker.getMaker().makeTaskInfos(foreachTask.getTasks(), taskInfo, groupIndex);
            Set<TaskInfo> subTaskInfos = new HashSet<>(taskInfoMap.values());

            Map<String, Object> subContext = Maps.newConcurrentMap();
            subContext.putAll(input);
            subContext.put(iterationMapping.getItem(), item);
            // record whether the subtask is key
            if (existKeyExp(taskInfo)) {
                for (TaskInfo subTaskInfo : subTaskInfos) {
                    boolean isKey = isKeySubTask(executionId, subContext, subTaskInfo);
                    if (existKeyExp(subTaskInfo) && isKey) {
                        indexToKey.put(String.valueOf(groupIndex), true);
                        break;
                    }
                }
            }

            taskInfo.getChildren().putAll(subTaskInfos.stream().collect(Collectors.toMap(TaskInfo::getName, it -> it)));
            indexToStatus.put(String.valueOf(groupIndex), TaskStatus.READY);
            updateGroupIdentity(executionId, item, taskInfo, iterationMapping.getIdentity(), groupIndex);

            Map<String, Object> groupedContext = Maps.newHashMap();
            groupedContext.put(DAGWalkHelper.getInstance().buildSubTaskContextFieldName(subTaskInfos.iterator().next().getRouteName()), subContext);
            contextToUpdate.putAll(groupedContext);
            if (maxConcurrentGroups <= 0 || groupIndex < maxConcurrentGroups) {
                readyToRun.add(Pair.of(subTaskInfos, groupedContext));
                indexToStatus.put(String.valueOf(groupIndex), TaskStatus.RUNNING);
            }
        });

        dagContextStorage.updateContext(executionId, contextToUpdate);
        dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));

        log.info("run foreach task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).subTaskInfosAndContext(readyToRun).build();
    }

    private boolean isKeySubTask(String executionId, Map<String, Object> subContext, TaskInfo it) {
        return !stasher.needStash(executionId, it, subContext);
    }

    private boolean existKeyExp(TaskInfo taskInfo) {
        return Optional.ofNullable(taskInfo).map(TaskInfo::getTask).map(BaseTask::getKeyExp).isPresent();
    }

    private int maxGroupsToRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        try {
            Synchronization synchronization = ((ForeachTask) taskInfo.getTask()).getSynchronization();
            if (synchronization == null ||
                    CollectionUtils.isEmpty(synchronization.getConditions()) ||
                    StringUtils.isBlank(synchronization.getMaxConcurrency())) {
                return -1;
            }

            boolean needSyncControl = conditionsAllMatch(synchronization.getConditions(), input, "input");
            if (!needSyncControl) {
                log.warn("maxGroupsToRun conditions mismatch executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
                return -1;
            }

            Map<String, Object> output = Maps.newHashMap();
            Mapping mapping = new Mapping(synchronization.getMaxConcurrency(), "$.output.maxConcurrency");
            inputMappings(Maps.newHashMap(), input, output, Lists.newArrayList(mapping));
            int maxConcurrency = Optional.ofNullable(output.get("maxConcurrency"))
                    .map(String::valueOf)
                    .map(Integer::valueOf)
                    .filter(it -> it > 0L)
                    .orElse(-1);
            log.warn("maxGroupsToRun executionId:{}, taskInfoName:{}, maxConcurrency:{}", executionId, taskInfo.getName(), maxConcurrency);
            return maxConcurrency;
        } catch (Exception e) {
            log.warn("maxGroupsToRun fails, executionId:{}, taskInfoName:{}, errorMsg:{}", executionId, taskInfo.getName(), e.getMessage());
            return -1;
        }
    }

    private void updateGroupIdentity(String executionId, Object item, TaskInfo taskInfo, String identity, Integer groupIndex) {
        try {
            if (StringUtils.isBlank(identity)) {
                return;
            }

            Map<String, Object> input = Maps.newHashMap();
            input.put("element", item);
            Map<String, Object> output = Maps.newHashMap();
            Mapping mapping = new Mapping(identity.replace("$.iteration.element", "$.input.element"), "$.output.identity");
            inputMappings(new HashMap<>(), input, output, Lists.newArrayList(mapping));
            String identityString = Optional.ofNullable(output.get("identity")).map(String::valueOf).orElse(null);

            if (StringUtils.isNotBlank(identityString)) {
                taskInfo.setSubGroupIndexToIdentity(Optional.ofNullable(taskInfo.getSubGroupIndexToIdentity()).orElse(Maps.newConcurrentMap()));
                taskInfo.getSubGroupIndexToIdentity().put(String.valueOf(groupIndex), identityString);
            }
        } catch (Exception e) {
            log.warn("updateGroupIdentity fails, executionId:{}, taskInfoName:{}, groupIndex:{}, errorMsg:{}",
                    executionId, taskInfo.getName(), groupIndex, e.getMessage());
        }
    }

    @Override
    public ExecutionResult finish(String executionId, NotifyInfo notifyInfo, Map<String, Object> output) {
        return finishParentTask(executionId, notifyInfo);
    }

}
