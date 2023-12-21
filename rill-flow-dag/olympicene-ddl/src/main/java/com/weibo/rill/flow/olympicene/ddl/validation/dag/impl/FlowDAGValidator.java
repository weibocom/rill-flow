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

package com.weibo.rill.flow.olympicene.ddl.validation.dag.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGType;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;
import com.weibo.rill.flow.olympicene.ddl.exception.DDLException;
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException;
import com.weibo.rill.flow.olympicene.ddl.validation.DAGValidator;
import com.weibo.rill.flow.olympicene.ddl.validation.TaskValidator;
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.TaskValidators;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Slf4j
public class FlowDAGValidator implements DAGValidator {
    private final List<TaskValidator<?>> taskValidators = Lists.newArrayList();
    private final Pattern namePattern = Pattern.compile("^[a-zA-Z0-9]+$");

    public FlowDAGValidator() {
        this.taskValidators.addAll(TaskValidators.allTaskValidations());
    }

    public FlowDAGValidator(List<TaskValidator<?>> taskValidators) {
        Optional.ofNullable(taskValidators).ifPresent(this.taskValidators::addAll);
    }

    @Override
    public boolean match(DAG target) {
        return target.getType() == DAGType.FLOW;
    }

    @Override
    public void validate(DAG dag) {
        try {
            if (dag == null || DAGType.FLOW != dag.getType()) {
                throw new DDLException(DDLErrorCode.DAG_DESCRIPTOR_INVALID.getCode(), "dag type invalid");
            }

            if (CollectionUtils.isEmpty(dag.getTasks())) {
                throw new DDLException(DDLErrorCode.DAG_TASK_EMPTY);
            }

            validateTaskDepth(1, dag.getTasks(), Lists.newArrayList());

            validateNames(dag);

            validateTaskRoute(dag.getTasks());

            Map<String, BaseResource> resourceMap = Optional.ofNullable(dag.getResources())
                    .map(resources -> resources.stream().collect(Collectors.toMap(BaseResource::getName, it -> it)))
                    .orElse(Collections.emptyMap());
            validateTasks(dag.getCommonMapping(), resourceMap, dag.getTasks());
        } catch (DDLException e) {
            log.warn("validate not pass, dag:{}", dag, e);
            throw e;
        }
    }

    /**
     * 若验证通过则: name符合命名规范 且 无重复
     */
    private void validateNames(DAG dag) {
        validateName(dag.getWorkspace());
        validateName(dag.getDagName());

        validateTaskNames(dag.getTasks(), Sets.newHashSet(dag.getWorkspace(), dag.getDagName()));
    }

    private void validateName(String name) {
        if (StringUtils.isEmpty(name) || name.length() > 100 || !namePattern.matcher(name).find()) {
            throw new ValidationException(DDLErrorCode.NAME_INVALID.getCode(), "name invalid:" + name);
        }
    }

    private void validateTaskNames(List<BaseTask> tasks, Set<String> allNames) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }

        tasks.forEach(task -> {
            String taskName = task.getName();

            validateName(taskName);
            if (allNames.contains(taskName)) {
                throw new ValidationException(DDLErrorCode.NAME_DUPLICATED.getCode(), "task name duplicated: " + taskName);
            }

            allNames.add(taskName);
            validateTaskNames(task.subTasks(), allNames);
        });
    }

    /**
     * 若验证通过则: 任务嵌套层数符合maxDepth要求
     */
    private void validateTaskDepth(int currentDepth, List<BaseTask> tasks, List<String> route) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }

        if (currentDepth > SystemConfig.getTaskMaxDepth()) {
            throw new ValidationException(DDLErrorCode.TASK_INVALID.getCode(), "exceed max depth: " + StringUtils.join(route, "->"));
        }

        tasks.forEach(task -> {
            List<String> currentRoute = Lists.newArrayList(route);
            currentRoute.add(task.getName());
            validateTaskDepth(currentDepth + 1, task.subTasks(), currentRoute);
        });
    }

    private void validateTaskRoute(List<BaseTask> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }

        Map<String, BaseTask> taskMap = tasks.stream().collect(Collectors.toMap(BaseTask::getName, it -> it));
        tasks.forEach(task -> validateTaskRoute(task.getName(), Lists.newArrayList(), taskMap));

        tasks.forEach(task -> {
            if (CollectionUtils.isNotEmpty(task.subTasks())) {
                validateTaskRoute(task.subTasks());
            }
        });
    }

    private void validateTaskRoute(String taskName, List<String> route, Map<String, BaseTask> taskMap) {
        List<String> currentRoute = Lists.newArrayList(route);
        if (currentRoute.contains(taskName)) {
            currentRoute.add(taskName);
            throw new ValidationException(DDLErrorCode.TASK_INVALID.getCode(), "circle found: " + StringUtils.join(currentRoute, "->"));
        }
        currentRoute.add(taskName);

        BaseTask task = taskMap.get(taskName);
        if (task == null) {
            throw new DDLException(DDLErrorCode.TASK_NEXT_INVALID.getCode(), String.format(DDLErrorCode.TASK_NEXT_INVALID.getMessage(), taskName));
        }
        String nextTaskNames = task.getNext();
        if (StringUtils.isEmpty(nextTaskNames)) {
            return;
        }

        Arrays.stream(nextTaskNames.split(",")).forEach(nextTaskName -> validateTaskRoute(nextTaskName, currentRoute, taskMap));
    }

    private void validateTasks(Map<String, List<Mapping>> commonMapping, Map<String, BaseResource> resourceMap, List<BaseTask> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }

        tasks.forEach(task -> taskValidators.stream()
                .filter(validator -> validator.match(task))
                .forEach(validator -> validator.validate(task, resourceMap))
        );
        tasks.forEach(task -> {
            List<Mapping> allMappings = Lists.newArrayList();
            Optional.ofNullable(task.getInputMappings()).ifPresent(allMappings::addAll);
            Optional.ofNullable(task.getOutputMappings()).ifPresent(allMappings::addAll);

            List<String> mappingReferences = allMappings.stream()
                    .map(Mapping::getReference)
                    .filter(Objects::nonNull)
                    .toList();

            mappingReferences.forEach(mappingReference -> {
                List<Mapping> mappings = commonMapping.get(mappingReference);
                if (CollectionUtils.isEmpty(mappings)) {
                    throw new ValidationException(DDLErrorCode.TASK_INVALID.getCode(), String.format("taskName: %s mappingReference: %s invalid", task.getName(), mappingReference));
                }
            });
        });

        tasks.forEach(task -> {
            if (CollectionUtils.isEmpty(task.subTasks())) {
                return;
            }

            if (!Objects.equals(task.getCategory(), TaskCategory.CHOICE.getValue())
                    && !Objects.equals(task.getCategory(), TaskCategory.FOREACH.getValue())) {
                throw new ValidationException(DDLErrorCode.TASK_INVALID.getCode(), task.getName() + " category invalid");
            }
            validateTasks(commonMapping, resourceMap, task.subTasks());
        });
    }
}
