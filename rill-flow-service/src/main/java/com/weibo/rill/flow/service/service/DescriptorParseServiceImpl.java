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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class DescriptorParseServiceImpl implements DescriptorParseService {
    private static final String CONTEXT_PREFIX = "$.context.";

    @Resource
    private DAGStringParser dagParser;

    @Override
    public void processWhenSetDAG(DAG dag) {
        Map<String, BaseTask> taskMap = getTaskMapByDag(dag);
        if (!processInputToGenerateInputMappings(dag, taskMap)) {
            return;
        }
        Map<String, List<List<String>>> taskPathsMap = processTaskInputMappings(dag, taskMap);
        LinkedHashMultimap<String, String> outputMappingsMultimap = getOutputMappingsByPaths(taskPathsMap);
        generateOutputMappingsIntoTasks(outputMappingsMultimap, taskMap);
    }

    private Map<String, List<List<String>>> processTaskInputMappings(DAG dag, Map<String, BaseTask> taskMap) {
        Map<String, List<List<String>>> taskPathsMap = new HashMap<>();
        for (BaseTask task : dag.getTasks()) {
            processTaskInputMapping(task, taskMap, taskPathsMap, dag.getEndTaskName());
        }
        return taskPathsMap;
    }

    private void processTaskInputMapping(BaseTask task, Map<String, BaseTask> taskMap,
                                         Map<String, List<List<String>>> taskPathsMap, String endTaskName) {
        for (Mapping inputMapping : task.getInputMappings()) {
            String[] elements = getSourcePathElementsByMapping(inputMapping);
            if (elements.length < 2) {
                continue;
            }
            String outputTaskName = elements[1];
            if (taskMap.containsKey(outputTaskName)) {
                updateInputMapping(inputMapping, outputTaskName, elements, taskPathsMap);
                if (task.getName().equalsIgnoreCase(endTaskName)) {
                    updateOutputTaskNext(taskMap.get(outputTaskName), endTaskName);
                }
            }
        }
    }

    private void updateInputMapping(Mapping inputMapping, String outputTaskName, String[] elements,
                                    Map<String, List<List<String>>> taskPathsMap) {
        inputMapping.setSource("$.context" + inputMapping.getSource().substring(1));
        taskPathsMap.computeIfAbsent(outputTaskName, k -> new ArrayList<>())
                .add(Arrays.asList(elements).subList(2, elements.length));
    }

    private void updateOutputTaskNext(BaseTask outputTask, String endTaskName) {
        String next = outputTask.getNext();
        if (StringUtils.isEmpty(next)) {
            outputTask.setNext(endTaskName);
        } else {
            Set<String> nextSet = new LinkedHashSet<>(Arrays.asList(next.split(",")));
            nextSet.add(endTaskName);
            outputTask.setNext(String.join(",", nextSet));
        }
    }

    private Map<String, BaseTask> getTaskMapByDag(DAG dag) {
        if (CollectionUtils.isEmpty(dag.getTasks())) {
            return Maps.newHashMap();
        }
        return dag.getTasks().stream().collect(Collectors.toMap(BaseTask::getName, Function.identity()));
    }

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

    private LinkedHashMultimap<String, String> getOutputMappingsByPaths(Map<String, List<List<String>>> taskPathsMap) {
        LinkedHashMultimap<String, String> result = LinkedHashMultimap.create();

        for (Map.Entry<String, List<List<String>>> commonPrefixesEntry: taskPathsMap.entrySet()) {
            String taskName = commonPrefixesEntry.getKey();
            List<List<String>> elementsList = commonPrefixesEntry.getValue();
            for (List<String> elements: elementsList) {
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
        }
        return result;
    }

    private boolean processInputToGenerateInputMappings(DAG dag, Map<String, BaseTask> taskMap) {
        if (CollectionUtils.isEmpty(dag.getTasks())) {
            return false;
        }
        PassTask endPassTask = generateEndPassTask(dag, taskMap);
        boolean taskExistsInput = endPassTask != null;
        for (BaseTask task : dag.getTasks()) {
            Map<String, Object> taskInput = task.getInput();
            if (MapUtils.isEmpty(taskInput)) {
                continue;
            } else {
                taskExistsInput = true;
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
        return taskExistsInput;
    }

    private PassTask generateEndPassTask(DAG dag, Map<String, BaseTask> taskMap) {
        if (MapUtils.isEmpty(dag.getOutput())) {
            return null;
        }
        String endTaskName = "endPassTask" + (new SimpleDateFormat("yyyyMMdd")).format(new Date());
        dag.setEndTaskName(endTaskName);
        PassTask endPassTask = new PassTask();
        endPassTask.setName(endTaskName);
        endPassTask.setInput(dag.getOutput());
        endPassTask.setCategory("pass");
        List<Mapping> outputMappings = Lists.newArrayList();
        for (String key : dag.getOutput().keySet()) {
            outputMappings.add(new Mapping("$.input." + key, CONTEXT_PREFIX + key));
        }
        endPassTask.setOutputMappings(outputMappings);

        taskMap.put(endTaskName, endPassTask);
        dag.getTasks().add(endPassTask);
        return endPassTask;
    }

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

    private void processInputMappingsWhenGetDescriptor(BaseTask task) {
        task.setInputMappings(null);
    }
}
