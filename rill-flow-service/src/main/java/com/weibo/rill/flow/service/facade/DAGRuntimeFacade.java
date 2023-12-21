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

package com.weibo.rill.flow.service.facade;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.Expression;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.function.ResourceStatus;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.DAGRecord;
import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.Progress;
import com.weibo.rill.flow.interfaces.model.task.*;
import com.weibo.rill.flow.olympicene.core.helper.DAGInfoMaker;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser;
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLMapper;
import com.weibo.rill.flow.olympicene.traversal.helper.ContextHelper;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping;
import com.weibo.rill.flow.service.component.DAGToolConverter;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import com.weibo.rill.flow.service.manager.AviatorCache;
import com.weibo.rill.flow.service.manager.DescriptorManager;
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic;
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic;
import com.weibo.rill.flow.service.storage.LongTermStorage;
import com.weibo.rill.flow.service.storage.RuntimeStorage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DAGRuntimeFacade {
    private static final String TASKS = "tasks";
    private static final String CONTEXT = "context";

    private final JSONPathInputOutputMapping mapping = new JSONPathInputOutputMapping();
    private final DAGWalkHelper dagWalkHelper = DAGWalkHelper.getInstance();

    @Autowired
    private DAGStringParser dagStringParser;
    @Autowired
    private RuntimeStorage runtimeStorage;
    @Autowired
    private LongTermStorage longTermStorage;
    @Autowired
    private DescriptorManager descriptorManager;
    @Autowired
    private DAGResourceStatistic dagResourceStatistic;
    @Autowired
    private HttpInvokeHelper httpInvokeHelper;
    @Autowired
    private TenantTaskStatistic tenantTaskStatistic;
    @Autowired
    private AviatorCache aviatorCache;
    @Autowired
    private OlympiceneFacade olympiceneFacade;

    public Map<String, Object> convertDAGInfo(String dagDescriptor) {
        try {
            DAG dag = dagStringParser.parse(dagDescriptor);
            DAGInfo dagInfo = new DAGInfoMaker().dag(dag).dagStatus(DAGStatus.NOT_STARTED).make();
            return makeDAGInfoMap(dagInfo, false);
        } catch (Exception e) {
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, e.getMessage());
        }
    }

    private Map<String, Object> makeDAGInfoMap(DAGInfo dagInfo, boolean brief) {
        Map<String, Object> ret = Maps.newHashMap();
        if (dagInfo == null) {
            ret.put(TASKS, "{}");
            return ret;
        }

        ret.put("dag_invoke_msg", dagInfo.getDagInvokeMsg());
        ret.put("dag_status", dagInfo.getDagStatus().name());
        ret.put("execution_id", dagInfo.getExecutionId());
        ret.put("process", dagProgressCalculate(dagInfo));
        if (!brief) {
            ret.put(TASKS, dagInfo.getTasks().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it -> DAGToolConverter.convertTaskInfo(it.getValue()))));
        }
        return ret;
    }

    private int dagProgressCalculate(DAGInfo dagInfo) {
        if (dagInfo.getDagStatus().isCompleted()) {
            return 100;
        }

        Collection<TaskInfo> tasks = Optional.ofNullable(dagInfo.getTasks()).map(Map::values).orElse(Collections.emptyList());
        if (CollectionUtils.isEmpty(tasks)) {
            log.info("dagProcessCalculate cannot get task map");
            return 0;
        }

        try {
            return doCalculateProgress(tasks);
        } catch (Exception e) {
            log.warn("calculate progress by default method, errorMsg:{}", e.getMessage());
            int allWeight = tasks.size();
            double completeWeight = tasks.stream().filter(task -> task.getTaskStatus().isCompleted()).count();
            return (int) (completeWeight * 100 / allWeight);
        }
    }

    private int doCalculateProgress(Collection<TaskInfo> tasks) {
        int allWeight = 0;
        double completeWeight = 0D;

        for (TaskInfo taskInfo : tasks) {
            Progress progress = taskInfo.getTask().getProgress();
            int weight = Optional.ofNullable(progress).map(Progress::getWeight).orElse(1);
            String calculation = Optional.ofNullable(progress).map(Progress::getCalculation).orElse(null);

            allWeight += weight;
            if (taskInfo.getTaskStatus().isCompleted()) {
                completeWeight += weight;
            } else if (StringUtils.isNotBlank(calculation)) {
                Map<String, Object> params = Optional.ofNullable(taskInfo.getTaskInvokeMsg())
                        .map(TaskInvokeMsg::getProgressArgs)
                        .orElse(Maps.newHashMap());
                long taskRunningTimeInMillis = Optional.ofNullable(taskInfo.getTaskInvokeMsg())
                        .map(TaskInvokeMsg::getInvokeTimeInfos)
                        .filter(CollectionUtils::isNotEmpty)
                        .map(it -> it.get(it.size() - 1))
                        .map(it -> System.currentTimeMillis() - it.getStartTimeInMillisecond())
                        .orElse(0L);
                Map<String, Object> env = Maps.newHashMap();
                env.put("params", params);
                env.put("taskRunningTimeInMillis", taskRunningTimeInMillis);

                Expression expression = aviatorCache.getAviatorExpression(calculation);
                String value = String.valueOf(expression.execute(env));
                if (NumberUtils.isParsable(value)) {
                    completeWeight += Double.parseDouble(value) * weight;
                }
            } else if (MapUtils.isNotEmpty(taskInfo.getSubGroupIndexToStatus())) {
                double allGroup = taskInfo.getSubGroupIndexToStatus().size();
                long completeGroup = taskInfo.getSubGroupIndexToStatus().values().stream().filter(TaskStatus::isCompleted).count();
                completeWeight += completeGroup * weight / allGroup;
            }
        }

        return (int) (completeWeight * 100 / allWeight);
    }

    public Map<String, Object> getBasicDAGInfo(String executionId, boolean brief) {
        DAGInfo dagInfo = runtimeStorage.getBasicDAGInfo(executionId);
        if (dagInfo == null) {
            dagInfo = longTermStorage.getBasicDAGInfo(executionId);
        }

        Map<String, Object> result = makeDAGInfoMap(dagInfo, brief);
        result.put("context", getContext(executionId, null));
        result.put("invoke_summary", tenantTaskStatistic.getFlowAggregate(executionId));
        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getContext(String executionId, String taskName) {
        if (StringUtils.isBlank(taskName) || dagWalkHelper.isAncestorTask(taskName)) {
            Map<String, Object> context = runtimeStorage.getContext(executionId);
            if (MapUtils.isEmpty(context)) {
                context = longTermStorage.getContext(executionId);
            }
            return Optional.ofNullable(context).orElse(Maps.newHashMap());
        }

        String subContextField = dagWalkHelper.buildSubTaskContextFieldName(dagWalkHelper.getRootName(taskName));
        Collection<String> fields = ImmutableSet.of(subContextField);
        Map<String, Object> groupedContext = runtimeStorage.getContext(executionId, fields);
        if (MapUtils.isEmpty(groupedContext)) {
            groupedContext = longTermStorage.getContext(executionId, fields);
        }
        return Optional.ofNullable(groupedContext).map(it -> (Map<String, Object>) it.get(subContextField)).orElse(Maps.newHashMap());
    }

    public Map<String, Object> getSubContext(String executionId, String parentTaskName, Integer groupIndex) {
        String routeName = dagWalkHelper.buildTaskInfoRouteName(parentTaskName, String.valueOf(groupIndex));
        String taskName = dagWalkHelper.buildTaskInfoName(routeName, "x");
        return getContext(executionId, taskName);
    }

    public Map<String, Object> getDAGInfoByParentName(String executionId, String parentTaskName, String groupIndex) {
        String routeName = dagWalkHelper.buildTaskInfoRouteName(parentTaskName, groupIndex);
        String taskName = dagWalkHelper.buildTaskInfoName(routeName, "x");

        TaskInfo taskInfo = null;
        try {
            taskInfo = runtimeStorage.getParentTaskInfoWithSibling(executionId, taskName);
        } catch (Exception e) {
            // do nothing
        }
        if (taskInfo == null) {
            taskInfo = longTermStorage.getTaskInfo(executionId, parentTaskName, groupIndex);
        }

        Map<String, Object> ret = Maps.newHashMap();
        if (taskInfo == null) {
            ret.put(TASKS, "{}");
            return ret;
        }
        ret.put(TASKS, taskInfo.getChildren().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it -> DAGToolConverter.convertTaskInfo(it.getValue()))));
        return ret;
    }

    public Map<String, Object> mappingEvaluation(String type, String mappingRules, String executionId, String taskName, JSONObject data) {
        try {
            List<Mapping> rules = Lists.newArrayList();
            if (StringUtils.isNotBlank(mappingRules)) {
                Mapping[] mappings = YAMLMapper.parseObject(mappingRules, Mapping[].class);
                rules.addAll(Lists.newArrayList(mappings));
            }

            Map<String, Object> context = Optional.ofNullable((Map<String, Object>) data.getJSONObject(CONTEXT))
                    .orElse(Maps.newHashMap());
            Map<String, Object> input = Optional.ofNullable((Map<String, Object>) data.getJSONObject("input"))
                    .orElse(Maps.newHashMap());
            Map<String, Object> output = Optional.ofNullable((Map<String, Object>) data.getJSONObject("output"))
                    .orElse(Maps.newHashMap());

            updateValue(type, executionId, taskName, rules, context, output);

            mapping.mapping(context, input, output, rules);

            return ImmutableMap.of(CONTEXT, context, "input", input, "output", output);
        } catch (Exception e) {
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, ExceptionUtils.getStackTrace(e));
        }
    }

    private void updateValue(String type, String executionId, String taskName, List<Mapping> rules, Map<String, Object> context, Map<String, Object> output) {
        if (StringUtils.isBlank(type) || (!type.equals("input_eva") && !type.equals("output_eva"))
                || StringUtils.isBlank(executionId) || StringUtils.isBlank(taskName)) {
            return;
        }

        Optional.ofNullable(context).filter(MapUtils::isEmpty).ifPresent(it -> it.putAll(getContext(executionId, taskName)));
        TaskInfo taskInfo = getBasicTaskInfo(executionId, taskName);

        if (type.equals("input_eva")) {
            Optional.ofNullable(rules).filter(CollectionUtils::isEmpty).ifPresent(it -> it.addAll(taskInfo.getTask().getInputMappings()));
        }
        if (type.equals("output_eva")) {
            Optional.ofNullable(rules).filter(CollectionUtils::isEmpty).ifPresent(it -> it.addAll(taskInfo.getTask().getOutputMappings()));

            if (MapUtils.isEmpty(output) && !Objects.equals(taskInfo.getTask().getCategory(), TaskCategory.CHOICE.getValue())
                    && !Objects.equals(taskInfo.getTask().getCategory(), TaskCategory.FOREACH.getValue())) {
                List<Map<String, Object>> subContextList = ContextHelper.getInstance().getSubContextList(runtimeStorage, executionId, taskInfo);
                output.put("sub_context", subContextList);
            }
        }
    }

    private TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        TaskInfo taskInfo = null;
        try {
            taskInfo = runtimeStorage.getBasicTaskInfo(executionId, taskName);
        } catch (Exception e) {
            // do nothing
        }
        if (taskInfo == null) {
            taskInfo = longTermStorage.getBasicTaskInfo(executionId, taskName);
        }
        if (taskInfo == null) {
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("can not get %s taskInfo execuitonId:%s", taskName, executionId));
        }
        return taskInfo;
    }

    public Map<String, Object> functionDispatchParams(String executionId, String taskName, JSONObject data) {
        try {
            AtomicReference<BaseTask> task = new AtomicReference<>();

            String resourceName = Optional.ofNullable(data.getString("resource_name"))
                    .orElseGet(() -> {
                        BaseTask baseTask = getBasicTaskInfo(executionId, taskName).getTask();
                        task.set(baseTask);
                        return baseTask instanceof FunctionTask ? ((FunctionTask) baseTask).getResourceName() : "http://mock.function.resource";
                    });
            List<Mapping> inputMappings = Optional.ofNullable(data.getString("input_mapping_rules"))
                    .map(it -> {
                        try {
                            Mapping[] mappings = YAMLMapper.parseObject(it, Mapping[].class);
                            return Lists.newArrayList(mappings);
                        } catch (IOException e) {
                            throw new TaskException(BizError.ERROR_DATA_FORMAT, e.getCause());
                        }
                    })
                    .orElseGet(() -> {
                        BaseTask baseTask = task.get() != null ? task.get() : getBasicTaskInfo(executionId, taskName).getTask();
                        return Lists.newArrayList(baseTask.getInputMappings());
                    });
            Map<String, Object> context = Optional.ofNullable((Map<String, Object>) data.getJSONObject(CONTEXT))
                    .orElseGet(() -> getContext(executionId, taskName));

            Resource resource = new Resource(resourceName);
            String reqExecutionId = Optional.ofNullable(executionId).orElse("mockExecutionId");
            String reqTaskName = Optional.ofNullable(taskName).orElse("mockTaskName");
            Map<String, Object> input = Maps.newHashMap();
            mapping.mapping(context, input, Maps.newHashMap(), inputMappings);
            HttpParameter requestParams = httpInvokeHelper.functionRequestParams(reqExecutionId, reqTaskName, resource, input);
            Map<String, Object> queryParams = requestParams.getQueryParams();
            Map<String, Object> body = requestParams.getBody();
            String url = httpInvokeHelper.buildUrl(resource, queryParams);
            return ImmutableMap.of("url", url, "body", body);
        } catch (Exception e) {
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, ExceptionUtils.getStackTrace(e));
        }
    }

    public Map<String, Object> dependencyCheck(String descriptorId, String descriptor) {
        String dagDescriptor = StringUtils.isNotBlank(descriptorId) ?
                descriptorManager.getDagDescriptor(0L, Collections.emptyMap(), descriptorId) : descriptor;
        DAG dag = dagStringParser.parse(dagDescriptor);
        Map<String, List<String>> dependencies = dagWalkHelper.getDependedResources(dag);
        List<Map<String, Object>> resourceToNames = dependencies.entrySet().stream()
                .map(entry -> ImmutableMap.of("resource_name", entry.getKey(), "names", entry.getValue()))
                .collect(Collectors.toList());
        return ImmutableMap.of("dependencies", resourceToNames);
    }

    public Map<String, Object> runtimeDependentResources(List<String> serviceIds) {
        Map<String, Object> ret = Maps.newHashMap();

        serviceIds.forEach(serviceId -> {
            Map<String, Map<String, ResourceStatus>> resourceOrder =
                    dagResourceStatistic.orderDependentResources(serviceId);
            ret.put(serviceId, resourceOrder);
        });
        ret.put("current_time", System.currentTimeMillis());

        return ret;
    }

    public Map<String, Object> clearRuntimeResources(String serviceId, boolean clearAll, List<String> resourceNames) {
        return ImmutableMap.of("ret", dagResourceStatistic.clearRuntimeResources(serviceId, clearAll, resourceNames));
    }

    public Map<String, Object> businessInvokeSummary(String businessKey) {
        return ImmutableMap.of("ret", tenantTaskStatistic.getBusinessAggregate(businessKey));
    }

    public Map<String, Object> getExecutionIds(String executionId, String business, String feature, String status, String code, Long startTime, Long endTime, Integer current, Integer pageSize) {
        if (StringUtils.isNotEmpty(executionId)) {
            DAGInfo dagInfo = runtimeStorage.getBasicDAGInfo(executionId);
            if (Objects.isNull(dagInfo)) {
                dagInfo = longTermStorage.getBasicDAGInfo(executionId);
            }
            if (Objects.isNull(dagInfo)){
                return Map.of("total", 0, "items", Lists.newArrayList());
            }
            JSONObject executionItem = new JSONObject();
            executionItem.put("id", 1);
            executionItem.put("execution_id", dagInfo.getExecutionId());
            executionItem.put("submit_time", dagInfo.getDagInvokeMsg().getInvokeTimeInfos().get(0).getStartTimeInMillisecond());
            executionItem.put("business_id", dagInfo.getDag().getWorkspace());
            executionItem.put("feature_id", dagInfo.getDag().getDagName());
            executionItem.put("status", dagInfo.getDagStatus());
            return Map.of("total", 1, "items", List.of(executionItem));
        }

        List<DAGRecord> dagRecordList = new ArrayList<>();
        if (StringUtils.isNotEmpty(business) && StringUtils.isNotEmpty(feature)) {
            dagRecordList.add(DAGRecord.builder()
                    .businessId(business)
                    .featureId(feature)
                    .build());
        } else if (StringUtils.isNotEmpty(business) && StringUtils.isEmpty(feature)) {
            descriptorManager.getFeature(business).forEach(featureId -> {
                DAGRecord record = DAGRecord.builder()
                        .businessId(business)
                        .featureId(featureId)
                        .build();
                dagRecordList.add(record);
            });
        } else {
            descriptorManager.getBusiness().forEach(businessId -> descriptorManager.getFeature(businessId).forEach(featureId -> {
                DAGRecord record = DAGRecord.builder()
                        .businessId(businessId)
                        .featureId(featureId)
                        .build();
                dagRecordList.add(record);
            }));
        }
        List<DAGStatus> dagStatuses = Lists.newArrayList();
        Optional.ofNullable(DAGStatus.parse(status))
                .ifPresentOrElse(
                        dagStatuses::add,
                        () -> dagStatuses.addAll(Arrays.asList(DAGStatus.values()))
                );
        long time = Optional.ofNullable(endTime).orElse(System.currentTimeMillis());

        List<JSONObject> executionCountList = new ArrayList<>();
        dagRecordList.forEach(record -> {
                    String serviceId = record.getBusinessId() + ":" + record.getFeatureId();
                    dagStatuses.forEach(dagStatus -> {
                                Map<String, Object> executionCount = olympiceneFacade.getExecutionIdsForBg(serviceId, dagStatus, code, time, current, pageSize);
                                JSONArray executionIds = Optional.ofNullable(executionCount)
                                        .map(JSONObject::new)
                                        .map(it -> it.getJSONArray("execution_ids"))
                                        .orElse(new JSONArray());
                                for (int i = 0; i < executionIds.size(); i++) {
                                    JSONObject executionInfo = executionIds.getJSONObject(i);
                                    JSONObject executionItem = new JSONObject();
                                    executionItem.put("id", i + 1);
                                    executionItem.put("execution_id", executionInfo.getString("execution_id"));
                                    executionItem.put("submit_time", executionInfo.get("submit_time"));
                                    executionItem.put("business_id", record.getBusinessId());
                                    executionItem.put("feature_id", record.getFeatureId());
                                    executionItem.put("status", dagStatus.getValue());
                                    executionCountList.add(executionItem);
                                }
                            });
                }
        );
        List<JSONObject> executionCountResult = executionCountList.stream()
                .filter(it -> (startTime.compareTo(it.getLong("submit_time"))) < 0)
                .sorted(Comparator.comparing(item -> item.getLong("submit_time"), (s, t) -> Long.compare(t, s)))
                .skip((long) (current - 1) * pageSize)
                .limit(pageSize)
                .toList();

        return Map.of("items", executionCountResult, "total", executionCountList.size());
    }
}
