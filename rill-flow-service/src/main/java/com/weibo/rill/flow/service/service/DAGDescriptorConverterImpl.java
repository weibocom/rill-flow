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

package com.weibo.rill.flow.service.service;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorDO;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO;
import com.weibo.rill.flow.olympicene.core.model.task.PassTask;
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class DAGDescriptorConverterImpl implements DAGDescriptorConverter {
    private static final String CONTEXT_PREFIX = "$.context.";
    private static final String INPUT_PREFIX = "$.input.";
    private static final String DAG_END_TASK_NAME = "endPassTask";

    @Autowired
    private DAGStringParser dagParser;

    @Override
    public DAG convertDescriptorDOToDAG(DescriptorDO descriptorDO) {
        return dagParser.parse(descriptorDO.getDescriptor());
    }

    @Override
    public DescriptorDO convertDAGToDescriptorDO(DAG dag) {
        String descriptor = dagParser.serialize(dag);
        return new DescriptorDO(descriptor);
    }

    @Override
    public DAG convertDescriptorVOToDAG(DescriptorVO descriptorVO) {
        DAG dag = dagParser.parse(descriptorVO.getDescriptor());
        Map<String, BaseTask> taskMap = getTaskMapByDag(dag);
        // 1. 处理 task 的 input 以及 dag 的 output，为任务生成原始的 inputMappings（input 中的来源直接作为 source，如 $.functionA.data.id）
        // 返回是否需要后续处理，不需要后续处理则直接返回
        if (!processInputToGenerateInputMappings(dag, taskMap)) {
            return dag;
        }
        // 2. 处理任务的 inputMappings，返回各任务 inputMappings 的 source 对应的元素列表的列表
        Map<String, List<List<String>>> taskPathsMap = processTaskInputMappings(dag, taskMap);
        // 3. 通过各个任务 inputMappings 对应的元素列表的列表，生成任务的 outputMappings
        LinkedHashMultimap<String, String> outputMappingsMultimap = getOutputMappingsByPaths(taskPathsMap);
        // 4. 将生成的 outputMappings 设置到对应的 task
        generateOutputMappingsIntoTasks(outputMappingsMultimap, taskMap);
        return dag;
    }

    @Override
    public DescriptorVO convertDAGToDescriptorVO(DAG dag) {
        // 1. 解析 descriptor，获取 taskName 到 task 的映射 map，并判断是否需要后续处理
        Map<String, BaseTask> taskMap = getTaskMapByDag(dag);
        if (!needsPostProcessing(dag, taskMap)) {
            return new DescriptorVO(dagParser.serialize(dag));
        }

        // 2. 对非结束节点的任务进行处理，包括 inputMappings、outputMappings 等的处理，同时在任务列表中删除 DAG 结束节点
        List<BaseTask> tasks = taskMap.values().stream().filter(task -> !task.getName().equals(dag.getEndTaskName()))
                .map(task -> processTask(task, dag.getEndTaskName())).toList();

        // 3. 重新序列化生成 descriptor
        dag.setTasks(tasks);
        dag.setEndTaskName(null);
        return new DescriptorVO(dagParser.serialize(dag));
    }

    /**
     * DAG获取任务名称到任务的映射
     */
    private Map<String, BaseTask> getTaskMapByDag(DAG dag) {
        if (CollectionUtils.isEmpty(dag.getTasks())) {
            return Maps.newHashMap();
        }
        return dag.getTasks().stream().collect(Collectors.toMap(BaseTask::getName, Function.identity()));
    }

    /**
     * 处理各个任务的 inputMappings，实现 inputMappings 的填充，返回 inputMappings 对应的元素列表的列表
     * @param dag DAG对象
     * @param taskMap 任务映射
     * @return 任务路径映射，如 functionB 任务的 inputMappings 包含两条，source 分别为：
     *         $["functionA"]["data"][0]["id"] 和 $["functionA"]["data"][0]["name"]
     *         则返回： ["functionB": [["data", "0", "id"], ["data", "1", "id"]]]
     */
    private Map<String, List<List<String>>> processTaskInputMappings(DAG dag, Map<String, BaseTask> taskMap) {
        Map<String, List<List<String>>> taskPathsMap = new HashMap<>();
        dag.getTasks().forEach(task -> processTaskInputMapping(task, taskMap, taskPathsMap, dag.getEndTaskName()));
        return taskPathsMap;
    }

    /**
     * 分析和处理任务的 inputMappings，并将结果填充到 taskPathsMap
     * @param task 待处理的任务
     * @param taskMap 任务映射
     * @param taskPathsMap 任务路径映射
     * @param endTaskName 结束任务名称
     */
    private void processTaskInputMapping(BaseTask task, Map<String, BaseTask> taskMap,
                                         Map<String, List<List<String>>> taskPathsMap, String endTaskName) {
        for (Mapping inputMapping : task.getInputMappings()) {
            List<String> elements = getSourcePathElementsByMapping(inputMapping);
            if (elements.size() < 2) {
                continue;
            }
            String outputTaskName = elements.get(1);
            if (taskMap.containsKey(outputTaskName)) {
                // 更新 inputMappings，并填充 taskPathsMap
                updateInputMapping(inputMapping, outputTaskName, elements, taskPathsMap);
                if (task.getName().equalsIgnoreCase(endTaskName)) {
                    // 如果当前任务为图的结束任务，则需要更新提供来源数据的任务的 next 属性，让它指向结束任务
                    updateTaskNext(taskMap.get(outputTaskName), endTaskName);
                }
            }
        }
    }

    /**
     * 更新 inputMappings，将 source 中的 $.functionA.id 变成 $.context.functionA.id，并填充 taskPathsMap
     * @param inputMapping 输入映射
     * @param outputTaskName 输出任务名称
     * @param elements 路径元素
     * @param taskPathsMap 任务路径映射
     */
    private void updateInputMapping(Mapping inputMapping, String outputTaskName, List<String> elements,
                                    Map<String, List<List<String>>> taskPathsMap) {
        inputMapping.setSource("$.context" + inputMapping.getSource().substring(1));
        taskPathsMap.computeIfAbsent(outputTaskName, k -> new ArrayList<>())
                .add(elements.subList(2, elements.size()));
    }

    /**
     * 将 taskName 放入到 task 的 next 中
     */
    private void updateTaskNext(BaseTask task, String taskName) {
        String next = task.getNext();
        if (StringUtils.isEmpty(next)) {
            task.setNext(taskName);
        } else {
            Set<String> nextSet = new LinkedHashSet<>(Arrays.asList(next.split(",")));
            nextSet.add(taskName);
            task.setNext(String.join(",", nextSet));
        }
    }

    /**
     * 将 inputMapping 中的 source 解析为 element 数组，如 $["functionA"]["data"]["ids"], 则返回 ["functionA", "data", "ids"]
     */
    private List<String> getSourcePathElementsByMapping(Mapping inputMapping) {
        if (inputMapping.getSource() == null || !inputMapping.getSource().startsWith("$.")) {
            return new ArrayList<>();
        }
        String source = inputMapping.getSource();
        String path;
        try {
            path = JsonPath.compile(source).getPath();
        } catch (Exception e) {
            return new ArrayList<>();
        }

        String normalizedPath = path.replace("\"", "'");
        return Arrays.stream(normalizedPath.split("\\['|']"))
                .filter(StringUtils::isNotEmpty)
                .toList();
    }

    /**
     * 将 outputMappings 设置到对应的任务中
     */
    private void generateOutputMappingsIntoTasks(LinkedHashMultimap<String, String> outputMappingsMultimap, Map<String, BaseTask> taskMap) {
        if (outputMappingsMultimap.isEmpty()) {
            return;
        }
        outputMappingsMultimap.forEach((taskName, path) -> {
            BaseTask task = taskMap.get(taskName);
            if (task != null) {
                List<Mapping> outputMappings = Optional.ofNullable(task.getOutputMappings()).orElse(new ArrayList<>());
                Set<String> targets = outputMappings.stream().map(Mapping::getTarget).collect(Collectors.toSet());
                String target = CONTEXT_PREFIX + taskName + path;
                if (!targets.contains(target)) {
                    outputMappings.add(new Mapping("$.output" + path, target));
                    task.setOutputMappings(outputMappings);
                }
            }
        });
    }

    /**
     * 根据 path 元素列表，生成任务的 outputMappings
     */
    private LinkedHashMultimap<String, String> getOutputMappingsByPaths(Map<String, List<List<String>>> taskPathsMap) {
        LinkedHashMultimap<String, String> result = LinkedHashMultimap.create();

        for (Map.Entry<String, List<List<String>>> taskPathElementsEntry: taskPathsMap.entrySet()) {
            String taskName = taskPathElementsEntry.getKey();
            List<List<String>> elementsList = taskPathElementsEntry.getValue();
            for (List<String> elements: elementsList) {
                processPathElements(elements, result, taskName);
            }
        }
        return result;
    }

    /**
     * 处理 jsonpath 对应的元素，并将结果设置到 result 中
     * 如 elements 为 ["$", "context", "functionA", "data", "text"]
     * 则生成的结果为：$.context.functionA.data.text
     */
    private void processPathElements(List<String> elements, LinkedHashMultimap<String, String> result, String taskName) {
        StringBuilder mappingSb = new StringBuilder();
        elements.forEach(element -> {
            if (element.contains(".")) {
                mappingSb.append("['").append(element).append("']");
            } else if (element.matches("\\[\\d+]") || element.equals("[*]")) {
                mappingSb.append(element);
            } else {
                mappingSb.append(".").append(element);
            }
        });
        if (!result.containsKey(mappingSb.toString())) {
            result.put(taskName, mappingSb.toString());
        }
    }

    /**
     * 根据 task 的 input 以及 dag 的 output，生成任务的 inputMappings
     * @return 是否通过新版本的任务 input 或 DAG output 配置，意即是否需要后续处理
     */
    private boolean processInputToGenerateInputMappings(DAG dag, Map<String, BaseTask> taskMap) {
        if (CollectionUtils.isEmpty(dag.getTasks())) {
            return false;
        }
        // 1. 根据 dag 的 output 生成图的 end 任务
        PassTask endPassTask = generateEndPassTask(dag, taskMap);
        boolean needPostProcess = endPassTask != null;
        // 2. 生成各个任务原始的 inputMappings，此时 source 内容仍然为 input 中的原始配置
        // 如 input 为 "id: $.functionA.id" 则生成的 inputMapping 的 source 为 $.functionA.id，target 为 $.input.id
        for (BaseTask task : dag.getTasks()) {
            Map<String, Object> taskInput = task.getInput();
            if (MapUtils.isEmpty(taskInput)) {
                continue;
            } else {
                needPostProcess = true;
            }
            List<Mapping> inputMappings = task.getInputMappings() == null ? new ArrayList<>() : task.getInputMappings();
            taskInput.entrySet().stream().filter(entry -> entry.getKey() != null && entry.getValue() != null).forEach(entry -> {
                String key = entry.getKey();
                Object value = entry.getValue();
                String target = INPUT_PREFIX + key;
                Mapping inputMapping;
                if (value instanceof Map) {
                    inputMapping = JSON.parseObject(JSON.toJSONString(value), Mapping.class);
                    inputMapping.setTarget(target);
                } else {
                    inputMapping = new Mapping(value.toString(), target);
                }
                inputMappings.add(inputMapping);
            });
            task.setInputMappings(inputMappings);
        }
        return needPostProcess;
    }

    /**
     * 生成 end 任务，用于设置图的最终输出信息
     */
    private PassTask generateEndPassTask(DAG dag, Map<String, BaseTask> taskMap) {
        if (MapUtils.isEmpty(dag.getOutput())) {
            return null;
        }
        dag.setEndTaskName(DAG_END_TASK_NAME);
        PassTask endPassTask = new PassTask();
        endPassTask.setName(DAG_END_TASK_NAME);
        endPassTask.setInput(dag.getOutput());
        endPassTask.setCategory("pass");
        // 由于图的 end 任务没有后续任务，所以需要生成它的 outputMappings 来实现将参数传递的信息放入到 context 中
        endPassTask.setOutputMappings(dag.getOutput().keySet().stream()
                .map(key -> new Mapping(INPUT_PREFIX + key, CONTEXT_PREFIX + key)).toList());

        taskMap.put(DAG_END_TASK_NAME, endPassTask);
        dag.getTasks().add(endPassTask);
        return endPassTask;
    }

    /**
     * 判断是否需要后续处理
     * @return 如果DAG有结束任务或任何一个任务有 input 配置，则返回true，否则返回false
     */
    private boolean needsPostProcessing(DAG dag, Map<String, BaseTask> taskMap) {
        return StringUtils.isNotBlank(dag.getEndTaskName()) ||
                taskMap.values().stream().anyMatch(task -> MapUtils.isNotEmpty(task.getInput()));
    }

    /**
     * 处理单个任务，包括 outputMappings、inputMappings、next 的处理
     */
    private BaseTask processTask(BaseTask task, String endTaskName) {
        // 1. 处理 outputMappings，删除自动生成的配置项
        processOutputMappingsWhenGetDescriptor(task);
        // 2. 处理 inputMappings，删除存在于 input 中的配置项
        processInputMappingsWhenGetDescriptor(task);
        // 3. 处理 next，删除指向 endTask 的指针
        removeFromNext(task, endTaskName);
        return task;
    }

    /**
     * 处理任务的 next 属性，删除指向 taskName 的指针
     */
    private void removeFromNext(BaseTask task, String taskName) {
        if (task.getNext() == null || StringUtils.isEmpty(taskName)) {
            return;
        }
        Set<String> nextSet = new LinkedHashSet<>(Arrays.asList(task.getNext().split(",")));
        nextSet.remove(taskName);
        task.setNext(String.join(",", nextSet));
    }

    /**
     * 处理获取描述符时的 outputMappings，删除自动生成的配置项
     */
    private void processOutputMappingsWhenGetDescriptor(BaseTask task) {
        List<Mapping> outputMappings = task.getOutputMappings();
        if (CollectionUtils.isEmpty(outputMappings)) {
            return;
        }
        List<Mapping> newOutputMappings = outputMappings.stream()
                .filter(mapping -> !mapping.getTarget().startsWith(CONTEXT_PREFIX + task.getName() + ".")).toList();
        task.setOutputMappings(CollectionUtils.isEmpty(newOutputMappings)? null: newOutputMappings);
    }

    /**
     * 处理获取描述符时的 inputMappings, 从任务的 inputMappings 中删除存在于 input 中的配置项
     */
    private void processInputMappingsWhenGetDescriptor(BaseTask task) {
        List<Mapping> inputMappings = task.getInputMappings();
        if (CollectionUtils.isEmpty(inputMappings)) {
            return;
        }
        Set<String> inputTargets = task.getInput().keySet().stream()
                .map(key -> INPUT_PREFIX + key).collect(Collectors.toSet());

        List<Mapping> filteredMappings = inputMappings.stream()
                .filter(mapping -> !inputTargets.contains(mapping.getTarget())).toList();

        task.setInputMappings(CollectionUtils.isEmpty(filteredMappings) ? null : filteredMappings);
    }
}
