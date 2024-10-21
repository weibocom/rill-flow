package com.weibo.rill.flow.service.service;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.task.PassTask;
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * descriptor 解析服务
 */
@Service
public class DescriptorParseServiceImpl implements DescriptorParseService {
    private static final String CONTEXT_PREFIX = "$.context.";

    @Resource
    private DAGStringParser dagParser;

    /**
     * 设置 DAG 时的处理
     * @param dag 待处理的DAG对象
     */
    @Override
    public void processWhenSetDAG(DAG dag) {
        // 1. 获取任务名称与任务的映射
        Map<String, BaseTask> taskMap = getTaskMapByDag(dag);
        // 2. 处理 task 的 input 以及 dag 的 output，为任务生成 inputMappings，返回是否需要后续处理，不需要后续处理则直接返回
        if (!processInputToGenerateInputMappings(dag, taskMap)) {
            return;
        }
        // 3. 处理任务的 inputMappings，返回各任务 inputMappings 的 source 对应的元素列表的列表
        Map<String, List<List<String>>> taskPathsMap = processTaskInputMappings(dag, taskMap);
        // 4. 通过各个任务 inputMappings 对应的元素列表的列表，生成任务的 outputMappings
        LinkedHashMultimap<String, String> outputMappingsMultimap = getOutputMappingsByPaths(taskPathsMap);
        // 5. 生成任务的输出映射
        generateOutputMappingsIntoTasks(outputMappingsMultimap, taskMap);
    }

    /**
     * 处理各个任务的 inputMappings，实现 inputMappings 的填充，返回 inputMappings 对应的元素列表的列表
     * @param dag DAG对象
     * @param taskMap 任务映射
     * @return 任务路径映射，如 functionB 任务的 inputMappings 包含两条，source 分别为：
     * $["functionA"]["data"][0]["id"] 和 $["functionA"]["data"][0]["name"]
     * 则返回： ["functionB": [["data", "0", "id"], ["data", "1", "id"]]]
     */
    private Map<String, List<List<String>>> processTaskInputMappings(DAG dag, Map<String, BaseTask> taskMap) {
        Map<String, List<List<String>>> taskPathsMap = new HashMap<>();
        for (BaseTask task : dag.getTasks()) {
            processTaskInputMapping(task, taskMap, taskPathsMap, dag.getEndTaskName());
        }
        return taskPathsMap;
    }

    /**
     * 分析和处理任务的 inputMappings
     * @param task 待处理的任务
     * @param taskMap 任务映射
     * @param taskPathsMap 任务路径映射
     * @param endTaskName 结束任务名称
     */
    private void processTaskInputMapping(BaseTask task, Map<String, BaseTask> taskMap,
                                         Map<String, List<List<String>>> taskPathsMap, String endTaskName) {
        for (Mapping inputMapping : task.getInputMappings()) {
            String[] elements = getSourcePathElementsByMapping(inputMapping);
            if (elements.length < 2) {
                continue;
            }
            String outputTaskName = elements[1];
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
    private void updateInputMapping(Mapping inputMapping, String outputTaskName, String[] elements,
                                    Map<String, List<List<String>>> taskPathsMap) {
        inputMapping.setSource("$.context" + inputMapping.getSource().substring(1));
        taskPathsMap.computeIfAbsent(outputTaskName, k -> new ArrayList<>())
                .add(Arrays.asList(elements).subList(2, elements.length));
    }

    /**
     * 将 taskName 放入到 task 的 next 中
     * @param task 任务
     * @param taskName 任务名称
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
     * 根据DAG获取任务映射
     * @param dag DAG对象
     * @return 任务名称到任务对象的映射
     */
    private Map<String, BaseTask> getTaskMapByDag(DAG dag) {
        if (CollectionUtils.isEmpty(dag.getTasks())) {
            return Maps.newHashMap();
        }
        return dag.getTasks().stream().collect(Collectors.toMap(BaseTask::getName, Function.identity()));
    }

    /**
     * 将 inputMapping 中的 source 解析为 element 数组，如 $["functionA"]["data"]["ids"], 则返回 ["functionA", "data", "ids"]
     * @param inputMapping 输入映射
     * @return jsonpath 元素数组
     */
    private String[] getSourcePathElementsByMapping(Mapping inputMapping) {
        if (inputMapping.getSource() == null || !inputMapping.getSource().startsWith("$.")) {
            return new String[0];
        }
        String source = inputMapping.getSource();
        String path;
        try {
            path = JsonPath.compile(source).getPath();
        } catch (Exception e) {
            return new String[0];
        }

        String normalizedPath = path.replace("\"", "'");
        return Arrays.stream(normalizedPath.split("\\['|']"))
                .filter(e -> !e.isEmpty())
                .toArray(String[]::new);
    }

    /**
     * 将输出映射生成到任务中
     * @param outputMappingsMultimap 输出映射多重映射
     * @param taskMap 任务映射
     */
    private void generateOutputMappingsIntoTasks(LinkedHashMultimap<String, String> outputMappingsMultimap, Map<String, BaseTask> taskMap) {
        if (outputMappingsMultimap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> mappingsEntry: outputMappingsMultimap.entries()) {
            String taskName = mappingsEntry.getKey();
            String path = mappingsEntry.getValue();
            BaseTask task = taskMap.get(taskName);
            if (task == null) {
                continue;
            }
            List<Mapping> outputMappings = task.getOutputMappings();
            if (outputMappings == null) {
                outputMappings = new ArrayList<>();
            }
            Set<String> targets = outputMappings.stream().map(Mapping::getTarget).collect(Collectors.toSet());
            String target = CONTEXT_PREFIX + taskName + path;
            if (!targets.contains(target)) {
                outputMappings.add(new Mapping("$.output" + path, target));
                task.setOutputMappings(outputMappings);
            }
        }
    }

    /**
     * 根据 path 元素列表，生成任务的 outputMappings
     * @param taskPathsMap 任务路径映射
     * @return 输出任务对应的 outputMappings
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
     * 处理 jsonpath 对应的元素
     * @param elements 路径元素列表
     * @param result 结果多重映射
     * @param taskName 任务名称
     */
    private void processPathElements(List<String> elements, LinkedHashMultimap<String, String> result, String taskName) {
        StringBuilder mappingSb = new StringBuilder();
        for (String element : elements) {
            if (element.contains(".")) {
                mappingSb.append("['").append(element).append("']");
            } else if (element.matches("\\[\\d+]") || element.equals("[*]")) {
                mappingSb.append(element);
            } else {
                mappingSb.append(".").append(element);
            }
        }
        if (!result.containsKey(mappingSb.toString())) {
            result.put(taskName, mappingSb.toString());
        }
    }

    /**
     * 处理 task 的 input 以及 dag 的 output，生成任务的 inputMappings
     * @param dag DAG对象
     * @param taskMap 任务映射
     * @return 是否通过新版本的任务 input 或 DAG output 配置
     */
    private boolean processInputToGenerateInputMappings(DAG dag, Map<String, BaseTask> taskMap) {
        if (CollectionUtils.isEmpty(dag.getTasks())) {
            return false;
        }
        // 1. 根据 dag 的 output 生成图的 end 任务
        PassTask endPassTask = generateEndPassTask(dag, taskMap);
        boolean needPostProcess = endPassTask != null;
        // 2. 生成各个任务原始的 inputMappings，此时 source 内容仍然为 input 中的原始配置
        // 如 input 为 id: $.functionA.id 则生成的 inputMapping 的source 为 $.functionA.id
        for (BaseTask task : dag.getTasks()) {
            Map<String, Object> taskInput = task.getInput();
            if (MapUtils.isEmpty(taskInput)) {
                continue;
            } else {
                needPostProcess = true;
            }
            List<Mapping> inputMappings = task.getInputMappings() == null ? Lists.newArrayList() : task.getInputMappings();
            for (Map.Entry<String, Object> entry : taskInput.entrySet()) {
                String target = "$.input." + entry.getKey();
                Mapping inputMapping;
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                } else if (entry.getValue() instanceof Map) {
                    inputMapping = JSON.parseObject(JSON.toJSONString(entry.getValue()), Mapping.class);
                    inputMapping.setTarget(target);
                } else {
                    inputMapping = new Mapping(entry.getValue().toString(), target);
                }
                inputMappings.add(inputMapping);
            }
            task.setInputMappings(inputMappings);
        }
        return needPostProcess;
    }

    /**
     * 生成 end 任务，放置图的最终输出信息
     * @param dag DAG对象
     * @param taskMap 图全部任务 map
     * @return 生成的 end 任务
     */
    private PassTask generateEndPassTask(DAG dag, Map<String, BaseTask> taskMap) {
        if (MapUtils.isEmpty(dag.getOutput())) {
            return null;
        }
        String endTaskName = "endPassTask";
        dag.setEndTaskName(endTaskName);
        PassTask endPassTask = new PassTask();
        endPassTask.setName(endTaskName);
        endPassTask.setInput(dag.getOutput());
        endPassTask.setCategory("pass");
        // 由于图的 end 任务没有后续任务，所以需要生成它的 outputMappings 来实现将参数传递的信息放入到 context 中
        List<Mapping> outputMappings = Lists.newArrayList();
        for (String key : dag.getOutput().keySet()) {
            outputMappings.add(new Mapping("$.input." + key, CONTEXT_PREFIX + key));
        }
        endPassTask.setOutputMappings(outputMappings);

        taskMap.put(endTaskName, endPassTask);
        dag.getTasks().add(endPassTask);
        return endPassTask;
    }

    /**
     * 处理获取描述符时的逻辑
     * @param descriptor 描述符字符串
     * @return 处理后的描述符字符串
     */
    @Override
    public String processWhenGetDescriptor(String descriptor) {
        DAG dag = dagParser.parse(descriptor);
        Map<String, BaseTask> taskMap = getTaskMapByDag(dag);
        boolean taskExistsInput = taskMap.values().stream()
                .anyMatch(task -> MapUtils.isNotEmpty(task.getInput()));
        if (!taskExistsInput) {
            return descriptor;
        }
        List<BaseTask> tasks = new ArrayList<>();
        for (BaseTask task : taskMap.values()) {
            if (task.getName().equals(dag.getEndTaskName())) {
                continue;
            }
            processOutputMappingsWhenGetDescriptor(task);
            processInputMappingsWhenGetDescriptor(task);
            if (task.getNext() != null) {
                String next = task.getNext();
                Set<String> nextSet = new LinkedHashSet<>(Arrays.asList(next.split(",")));
                nextSet.remove(dag.getEndTaskName());
                next = String.join(",", nextSet);
                task.setNext(next);
            }
            tasks.add(task);
        }
        dag.setTasks(tasks);
        dag.setEndTaskName(null);
        return dagParser.serialize(dag);
    }

    /**
     * 处理获取描述符时的 outputMappings
     * @param task 待处理的任务
     */
    private void processOutputMappingsWhenGetDescriptor(BaseTask task) {
        List<Mapping> outputMappings = task.getOutputMappings();
        if (CollectionUtils.isEmpty(outputMappings)) {
            return;
        }
        List<Mapping> newOutputMappings = outputMappings.stream()
                .filter(mapping -> !mapping.getTarget().startsWith(CONTEXT_PREFIX + task.getName()))
                .toList();
        if (CollectionUtils.isEmpty(newOutputMappings)) {
            newOutputMappings = null;
        }
        task.setOutputMappings(newOutputMappings);
    }

    /**
     * 处理获取描述符时的 inputMappings
     * @param task 待处理的任务
     */
    private void processInputMappingsWhenGetDescriptor(BaseTask task) {
        task.setInputMappings(null);
    }
}
