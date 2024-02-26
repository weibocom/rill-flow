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
import com.jayway.jsonpath.JsonPath;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.helper.TaskInfoMaker;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.task.Choice;
import com.weibo.rill.flow.olympicene.core.model.task.ChoiceTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import com.weibo.rill.flow.interfaces.model.task.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class ChoiceTaskRunner extends AbstractTaskRunner {
    public ChoiceTaskRunner(InputOutputMapping inputOutputMapping,
                            DAGContextStorage dagContextStorage,
                            DAGInfoStorage dagInfoStorage,
                            DAGStorageProcedure dagStorageProcedure,
                            SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.CHOICE;
    }

    @Override
    public boolean isEnable() {
        return false;
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("choice task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());
        ChoiceTask choiceTask = (ChoiceTask) taskInfo.getTask();

        List<Choice> choices = choiceTask.getChoices();
        if (CollectionUtils.isEmpty(choices)) {
            TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().msg("choices collection empty").build();
            taskInfo.updateInvokeMsg(taskInvokeMsg);
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
            dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
        }

        log.info("choice group size:{} executionId:{}, taskInfoName:{}", choices.size(), executionId, taskInfo.getName());
        Map<String, TaskStatus> indexToStatus = Maps.newConcurrentMap();
        taskInfo.setSubGroupIndexToStatus(indexToStatus);
        taskInfo.setTaskStatus(TaskStatus.RUNNING);

        AtomicInteger index = new AtomicInteger(0);
        List<Pair<Set<TaskInfo>, Map<String, Object>>> subTaskInfosAndContext = Lists.newArrayList();
        choices.stream()
                .sorted((a, b) -> a.getCondition().compareToIgnoreCase(b.getCondition()))
                .forEach(it -> {
                    int groupIndex = index.getAndIncrement();
                    Map<String, TaskInfo> taskInfoMap = TaskInfoMaker.getMaker().makeTaskInfos(it.getTasks(), taskInfo, groupIndex);
                    Set<TaskInfo> subTaskInfos = new HashSet<>(taskInfoMap.values());

                    taskInfo.setChildren(Optional.ofNullable(taskInfo.getChildren()).orElse(Maps.newConcurrentMap()));
                    taskInfo.getChildren().putAll(subTaskInfos.stream().collect(Collectors.toMap(TaskInfo::getName, e -> e)));
                    indexToStatus.put(String.valueOf(groupIndex), TaskStatus.READY);

                    boolean condition = false;
                    try {
                        List<String> result = JsonPath.using(valuePathConf).parse(ImmutableMap.of("input", input)).read(it.getCondition());
                        condition = !result.isEmpty();
                    } catch (Exception e) {
                        log.warn("choiceTask {} evaluation condition expression {} exception. ", taskInfo.getName(), it.getCondition(), e);
                    }
                    if (condition) {
                        Map<String, Object> subContext = Maps.newConcurrentMap();
                        subContext.putAll(input);
                        Map<String, Object> groupedContext = Maps.newHashMap();
                        groupedContext.put(DAGWalkHelper.getInstance().buildSubTaskContextFieldName(subTaskInfos.iterator().next().getRouteName()), subContext);
                        subTaskInfosAndContext.add(Pair.of(subTaskInfos, groupedContext));
                    } else {
                        subTaskInfos.forEach(t -> t.setTaskStatus(TaskStatus.SKIPPED));
                        taskInfo.getSubGroupIndexToStatus().put(String.valueOf(groupIndex), TaskStatus.SKIPPED);
                    }
                });

        Map<String, Object> contextToUpdate = subTaskInfosAndContext.stream()
                .map(Pair::getRight).flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2));
        TaskStatus taskStatus = DAGWalkHelper.getInstance().calculateParentStatus(taskInfo);
        if (taskStatus.isCompleted()) {
            taskInfo.setTaskStatus(taskStatus);
            updateTaskInvokeEndTime(taskInfo);
        }
        dagContextStorage.updateContext(executionId, contextToUpdate);
        dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
        log.info("run choice task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        return ExecutionResult.builder()
                .taskStatus(taskInfo.getTaskStatus())
                .subTaskInfosAndContext(subTaskInfosAndContext)
                .build();
    }

    @Override
    public ExecutionResult finish(String executionId, NotifyInfo notifyInfo, Map<String, Object> output) {
        return finishParentTask(executionId, notifyInfo);
    }

    @Override
    protected Map<String, Object> getSubTaskContextMap(String executionId, TaskInfo taskInfo) {
        List<Map<String, Object>> subContextList = ContextHelper.getInstance().getSubContextList(dagContextStorage, executionId, taskInfo);
        Map<String, Object> output = Maps.newConcurrentMap();
        subContextList.stream().filter(MapUtils::isNotEmpty).forEach(output::putAll);
        return output;
    }
}
