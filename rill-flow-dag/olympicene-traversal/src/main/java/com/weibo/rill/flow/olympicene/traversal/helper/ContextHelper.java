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

package com.weibo.rill.flow.olympicene.traversal.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class ContextHelper {
    private static final ContextHelper INSTANCE = new ContextHelper();

    public static ContextHelper getInstance() {
        return INSTANCE;
    }

    @Getter
    @Setter
    private volatile boolean independentContext = true;

    @SuppressWarnings("unchecked")
    public Map<String, Object> getContext(DAGContextStorage dagContextStorage, String executionId, TaskInfo taskInfo) {
        Map<String, Object> context;
        if (taskInfo != null && !DAGWalkHelper.getInstance().isAncestorTask(taskInfo.getName())) {
            String filed = DAGWalkHelper.getInstance().buildSubTaskContextFieldName(taskInfo.getRouteName());
            Map<String, Object> subContext = dagContextStorage.getContext(executionId, ImmutableSet.of(filed));

            context = Maps.newConcurrentMap();
            context.putAll((Map<String, Object>) subContext.get(filed));
        } else {
            context = dagContextStorage.getContext(executionId);
        }

        return Optional.ofNullable(context)
                .orElseThrow(() -> new DAGTraversalException(TraversalErrorCode.TRAVERSAL_FAILED.getCode(), "context is null"));
    }

    public List<Pair<TaskInfo, Map<String, Object>>> getContext(DAGContextStorage dagContextStorage, String executionId, Set<TaskInfo> taskInfos) {
        Map<String, Object> groupedContext = groupedContextByTaskInfos(dagContextStorage, executionId, taskInfos);
        return getContext(taskInfos, groupedContext);
    }

    public List<Pair<TaskInfo, Map<String, Object>>> getContext(Set<TaskInfo> taskInfos, Map<String, Object> groupedContext) {
        Map<String, Object> gContext = Optional.ofNullable(groupedContext).orElse(Maps.newHashMap());
        return independentContext ? getIndependentContext(taskInfos, gContext) : getSharedContext(taskInfos, gContext);
    }

    @SuppressWarnings("unchecked")
    private List<Pair<TaskInfo, Map<String, Object>>> getSharedContext(Set<TaskInfo> taskInfos, Map<String, Object> gContext) {
        return taskInfos.stream().map(taskInfo -> {
            Map<String, Object> context;
            if (DAGWalkHelper.getInstance().isAncestorTask(taskInfo.getName())) {
                context = gContext.entrySet().stream()
                        .filter(entry -> !DAGWalkHelper.getInstance().isSubContextFieldName(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else {
                context = Maps.newHashMap();
                String field = DAGWalkHelper.getInstance().buildSubTaskContextFieldName(taskInfo.getRouteName());
                Optional.ofNullable(gContext.get(field)).map(it -> (Map<String, Object>) it).ifPresent(context::putAll);
            }
            return Pair.of(taskInfo, context);
        }).toList();
    }

    @SuppressWarnings("unchecked")
    private List<Pair<TaskInfo, Map<String, Object>>> getIndependentContext(Set<TaskInfo> taskInfos, Map<String, Object> gContext) {
        List<TaskInfo> ancestorTasks = Lists.newArrayList();
        Map<String, List<TaskInfo>> fieldToSubTasks = Maps.newHashMap();
        taskInfos.forEach(taskInfo -> {
            if (DAGWalkHelper.getInstance().isAncestorTask(taskInfo.getName())) {
                ancestorTasks.add(taskInfo);
            } else {
                String field = DAGWalkHelper.getInstance().buildSubTaskContextFieldName(taskInfo.getRouteName());
                List<TaskInfo> subTasks = fieldToSubTasks.computeIfAbsent(field, it -> Lists.newArrayList());
                subTasks.add(taskInfo);
            }
        });

        List<Pair<TaskInfo, Map<String, Object>>> ret = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(ancestorTasks)) {
            Map<String, Object> ancestorContext = gContext.entrySet().stream()
                    .filter(entry -> !DAGWalkHelper.getInstance().isSubContextFieldName(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            ret.addAll(calculateIndependentContext(ancestorTasks, ancestorContext));
        }
        fieldToSubTasks.forEach((field, subTaskInfos) -> {
            Map<String, Object> fieldContext = Maps.newHashMap();
            Optional.ofNullable(gContext.get(field)).map(it -> (Map<String, Object>) it).ifPresent(fieldContext::putAll);
            ret.addAll(calculateIndependentContext(subTaskInfos, fieldContext));
        });

        return ret;
    }

    @SuppressWarnings("unchecked")
    private List<Pair<TaskInfo, Map<String, Object>>> calculateIndependentContext(List<TaskInfo> tasks, Map<String, Object> sharedContext) {
        if (CollectionUtils.isEmpty(tasks)) {
            return Collections.emptyList();
        }

        if (tasks.size() == 1) {
            return tasks.stream().map(taskInfo -> Pair.of(taskInfo, sharedContext)).toList();
        }

        byte[] sharedContextBytes = DAGTraversalSerializer.serializeToString(sharedContext).getBytes(StandardCharsets.UTF_8);
        return tasks.stream().map(taskInfo -> {
            Map<String, Object> context = (Map<String, Object>) DAGTraversalSerializer.deserialize(sharedContextBytes, Map.class);
            return Pair.of(taskInfo, context);
        }).toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getSubContextList(DAGContextStorage dagContextStorage, String executionId, TaskInfo taskInfo) {
        Set<String> subContextFields = DAGWalkHelper.getInstance().buildSubTaskContextFieldNameInCurrentTask(taskInfo);
        if (CollectionUtils.isEmpty(subContextFields)) {
            return Lists.newArrayList();
        }

        Map<String, Object> groupedContext = dagContextStorage.getContext(executionId, subContextFields);
        return groupedContext.values().stream().map(context -> (Map<String, Object>) context).toList();
    }

    /**
     * 分组获取每种类型的context
     * subTask中routeName相同的对应相同的context
     * 最外层对应一种context
     */
    public Map<String, Object> groupedContextByTaskInfos(DAGContextStorage dagContextStorage, String executionId, Set<TaskInfo> readyToRunTasks) {
        Set<TaskInfo> allTaskInfos = readyToRunTasks.stream().filter(Objects::nonNull).collect(Collectors.toSet());
        Set<TaskInfo> subTaskInfos = allTaskInfos.stream()
                .filter(taskInfo -> !DAGWalkHelper.getInstance().isAncestorTask(taskInfo.getName()))
                .collect(Collectors.toSet());

        Map<String, Object> groupedContext = Maps.newHashMap();

        if (!subTaskInfos.isEmpty()) {
            groupedContext.putAll(dagContextStorage.getContext(executionId, DAGWalkHelper.getInstance().buildSubTaskContextFieldName(subTaskInfos)));
        }

        if (allTaskInfos.size() != subTaskInfos.size()) {
            groupedContext.putAll(dagContextStorage.getContext(executionId));
        }

        return groupedContext;
    }

    private ContextHelper() {

    }
}
