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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.constant.ReservedConstant;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.function.ResourceCheckConfig;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.BusinessHeapStatus;
import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.interfaces.model.strategy.Degrade;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.DAGSettings;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser;
import com.weibo.rill.flow.olympicene.traversal.Olympicene;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import com.weibo.rill.flow.service.context.DAGContextInitializer;
import com.weibo.rill.flow.service.invoke.DAGFlowRedo;
import com.weibo.rill.flow.service.manager.DescriptorManager;
import com.weibo.rill.flow.service.statistic.DAGResourceStatistic;
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker;
import com.weibo.rill.flow.service.statistic.ProfileRecordService;
import com.weibo.rill.flow.service.statistic.SystemMonitorStatistic;
import com.weibo.rill.flow.service.storage.CustomizedStorage;
import com.weibo.rill.flow.service.storage.LongTermStorage;
import com.weibo.rill.flow.service.storage.RuntimeStorage;
import com.weibo.rill.flow.service.util.DescriptorIdUtil;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@Service
public class OlympiceneFacade {
    private static final String RESULT = "result";
    private static final String CODE = "code";

    @Autowired
    private DAGStringParser dagStringParser;
    @Autowired
    private Olympicene olympicene;
    @Autowired
    private DescriptorManager descriptorManager;
    @Autowired
    private SystemMonitorStatistic systemMonitorStatistic;
    @Autowired
    private RuntimeStorage runtimeStorage;
    @Autowired
    private LongTermStorage longTermStorage;
    @Autowired
    private DAGResourceStatistic dagResourceStatistic;
    @Autowired
    private CustomizedStorage customizedStorage;
    @Autowired
    private DAGFlowRedo dagFlowRedo;
    @Autowired
    private ProfileRecordService profileRecordService;
    @Autowired
    private DAGSubmitChecker dagSubmitChecker;
    @Autowired
    private DAGContextInitializer dagContextInitializer;

    public Map<String, Object> submit(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject data, String url) {
        Supplier<Map<String, Object>> submitActions = () -> {
            ResourceCheckConfig resourceCheckConfig = dagSubmitChecker.getCheckConfig(resourceCheck);
            String businessId = DescriptorIdUtil.changeDescriptorIdToBusinessId(descriptorId);
            Map<String, Object> context = dagContextInitializer.newSubmitContextBuilder(businessId).withData(data).withIdentity(descriptorId).build();

            return submit(uid, descriptorId, context, callback, resourceCheckConfig);
        };

        return profileRecordService.runNotifyAndRecordProfile(url, descriptorId, submitActions);
    }


    public Map<String, Object> submit(User flowUser, String descriptorId, Map<String, Object> context, String callback, ResourceCheckConfig resourceCheckConfig) {
        return submit(Optional.ofNullable(flowUser).map(User::getUid).orElse(0L), descriptorId, context, callback, resourceCheckConfig);
    }

    public Map<String, Object> submit(Long uid, String descriptorId, Map<String, Object> context, String callback, ResourceCheckConfig resourceCheckConfig) {
        String dagDescriptor = descriptorManager.getDagDescriptor(uid, context, descriptorId);
        DAG dag = dagStringParser.parse(dagDescriptor);
        String executionId = ExecutionIdUtil.generateExecutionId(dag);

        dagSubmitChecker.check(executionId, resourceCheckConfig);

        NotifyInfo notifyInfo = null;
        if (StringUtils.isNotBlank(callback)) {
            notifyInfo = NotifyInfo.builder()
                    .callbackConfig(DAGTraversalSerializer.deserialize(callback.getBytes(StandardCharsets.UTF_8), CallbackConfig.class))
                    .build();
        }
        context.put("flow_execution_id", executionId);
        olympicene.submit(executionId, dag, context, DAGSettings.DEFAULT, notifyInfo);
        Map<String, Object> ret = Maps.newHashMap();
        ret.put("execution_id", executionId);
        return ret;
    }

    public Map<String, Object> finish(String executionId, Map<String, Object> data, JSONObject rawCallbackData) {
        JSONObject passThrough = rawCallbackData.getJSONObject("passthrough");
        String taskName = passThrough.getString("task_name");
        TaskStatus taskStatus = "SUCCESS".equalsIgnoreCase(rawCallbackData.getString("result_type")) ? TaskStatus.SUCCEED : TaskStatus.FAILED;
        TaskInvokeMsg taskInvokeMsg = extractInvokeMsg(rawCallbackData);

        NotifyInfo notifyInfo = NotifyInfo.builder()
                .taskInfoName(taskName)
                .taskStatus(taskStatus)
                .taskInvokeMsg(taskInvokeMsg)
                .build();
        olympicene.finish(executionId, DAGSettings.DEFAULT, data, notifyInfo);
        dagResourceStatistic.updateUrlTypeResourceStatus(executionId, taskName, passThrough.getString("resource_name"), rawCallbackData);

        return ImmutableMap.of(RESULT, "ok");
    }

    private TaskInvokeMsg extractInvokeMsg(JSONObject data) {
        TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().build();

        String code = data.containsKey("error_code") ? data.getString("error_code") : data.getString(CODE);
        taskInvokeMsg.setCode(code);

        String msg = data.containsKey("error_msg") ? data.getString("error_msg") : data.getString("msg");
        taskInvokeMsg.setMsg(msg);

        taskInvokeMsg.setInvokeId(data.getString("invoke_id"));

        Map<String, Object> ext = Maps.newHashMap();
        Optional.ofNullable(data.getJSONObject("error_detail"))
                .filter(MapUtils::isNotEmpty)
                .ifPresent(errorDetail -> ext.put("error_detail", errorDetail));
        Optional.ofNullable(data.getJSONObject("context"))
                .filter(MapUtils::isNotEmpty)
                .ifPresent(context -> ext.put("context", context));
        taskInvokeMsg.setExt(ext);

        return taskInvokeMsg;
    }

    public Map<String, Object> wakeup(String executionId, String taskName, Map<String, Object> data) {
        olympicene.wakeup(executionId, data, NotifyInfo.builder().taskInfoName(taskName).build());
        return ImmutableMap.of(RESULT, "ok");
    }

    public Map<String, Object> redo(String executionId, List<String> taskNames, Map<String, Object> data) {
        NotifyInfo notifyInfo = NotifyInfo.builder().taskInfoNames(taskNames).build();

        try {
            ensureRuntimeLoaded(executionId);
            olympicene.redo(executionId, data, notifyInfo);
        } catch (DAGTraversalException e) {
            if (e.getErrorCode() != TraversalErrorCode.DAG_NOT_FOUND.getCode()) {
                throw e;
            }

            // 运行时存储中不存在 若保存在长期存储中 则加载到运行时存储
            DAGInfo dagInfo = longTermStorage.getDAGInfo(executionId);
            if (dagInfo == null) {
                log.warn("redo longTerm storage do not save current dagInfo, executionId:{}", executionId);
                throw e;
            }
            Map<String, Object> context = longTermStorage.getContext(executionId);
            log.info("redo reload dag runtime content, context size:{}, executionId:{}", context.size(), executionId);
            runtimeStorage.saveDAGInfo(executionId, dagInfo);
            runtimeStorage.updateContext(executionId, context);
            olympicene.redo(executionId, data, notifyInfo);
        }

        return ImmutableMap.of(RESULT, "ok");
    }

    private void ensureRuntimeLoaded(String executionId) {
        if (runtimeStorage.getDAGInfo(executionId) == null) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_NOT_FOUND.getCode(), "can not find dag info before redo.");
        }
    }

    public void multiRedo(String serviceId, DAGStatus dagStatus, String code, long cursor, Integer count, List<String> taskNames, Integer rate) {
        List<Pair<String, String>> ret;
        if (StringUtils.isBlank(code)) {
            ret = systemMonitorStatistic.getExecutionIdsByStatus(serviceId, dagStatus, cursor, 0, count);
        } else {
            ret = systemMonitorStatistic.getExecutionIdsByCode(serviceId, code, cursor, 0, count);
        }
        List<String> executionIds = ret.stream().map(Pair::getLeft).toList();
        dagFlowRedo.redoFlowWithTrafficLimit(executionIds, taskNames, rate);
    }

    public Map<String, Object> taskDegrade(String executionId, String taskName, boolean degradeCurrentTask, boolean degradeFollowingTasks) {
        DAG dag = runtimeStorage.getDAGDescriptor(executionId);
        List<String> chainBaseNames = DAGWalkHelper.getInstance().taskInfoNamesCurrentChain(taskName).stream()
                .map(taskInfoName -> DAGWalkHelper.getInstance().getBaseTaskName(taskInfoName))
                .toList();

        BaseTask baseTask = null;
        List<BaseTask> tasks = dag.getTasks();
        for (String baseName : chainBaseNames) {
            baseTask = tasks.stream()
                    .filter(task -> baseName.equals(task.getName()))
                    .findFirst()
                    .orElse(null);
            if (baseTask == null) {
                break;
            }
            tasks = baseTask.subTasks();
        }
        if (baseTask == null) {
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(),
                    String.format("can not find base task:%s, executionId:%s", taskName, executionId));
        }

        boolean originalCurrent = Optional.ofNullable(baseTask.getDegrade()).map(Degrade::getCurrent).orElse(false);
        boolean originalFollowings = Optional.ofNullable(baseTask.getDegrade()).map(Degrade::getFollowings).orElse(false);
        boolean configChanged = degradeCurrentTask != originalCurrent || degradeFollowingTasks != originalFollowings;
        if (configChanged) {
            Degrade degrade = new Degrade();
            degrade.setCurrent(degradeCurrentTask);
            degrade.setFollowings(degradeFollowingTasks);
            baseTask.setDegrade(degrade);
            runtimeStorage.updateDAGDescriptor(executionId, dag);
        }
        return ImmutableMap.of(RESULT, "ok");
    }

    public JSONObject businessHeapMonitor(List<String> serviceIds, Integer startTimeOffset, Integer endTimeOffset) {
        return systemMonitorStatistic.businessHeapMonitor(serviceIds, startTimeOffset, endTimeOffset);
    }

    public Map<String, Object> getExecutionCount(String serviceId, DAGStatus dagStatus, String code, Integer startTimeOffset, Integer endTimeOffset) {
        BusinessHeapStatus businessHeapStatus = systemMonitorStatistic.calculateTimePeriod(serviceId, startTimeOffset, endTimeOffset);

        Map<String, Object> ret = Maps.newHashMap();
        Optional.ofNullable(dagStatus).ifPresent(it -> ret.put("dag", systemMonitorStatistic.getExecutionCountByStatus(businessHeapStatus, it)));
        Optional.ofNullable(code).filter(StringUtils::isNotBlank).ifPresent(it -> ret.put(CODE, systemMonitorStatistic.getExecutionCountByCode(businessHeapStatus, it)));
        if (MapUtils.isEmpty(ret)) {
            ret.put("dag", systemMonitorStatistic.getExecutionCountByStatus(businessHeapStatus, null));
            ret.put(CODE, systemMonitorStatistic.getExecutionCountByCode(businessHeapStatus, null));
        }
        ret.put("collect_time", businessHeapStatus.getCollectTime());

        return ret;
    }

    public Map<String, Object> getExecutionIds(String serviceId, DAGStatus dagStatus, String code, Long time, Integer page, Integer count) {
        int offset = (page - 1) * count;
        List<Pair<String, String>> ids;
        String type;
        if (StringUtils.isBlank(code)) {
            ids = systemMonitorStatistic.getExecutionIdsByStatus(serviceId, dagStatus, time, offset, count + 1);
            type = dagStatus.getValue();
        } else {
            ids = systemMonitorStatistic.getExecutionIdsByCode(serviceId, code, time, offset, count + 1);
            type = code;
        }

        boolean nextPageAvailable = ids.size() > count;
        List<Map<String, Object>> executionIds = ids.stream().limit(count)
                .map(it -> Map.of("execution_id", (Object) it.getLeft(), "submit_time", Long.valueOf(it.getRight())))
                .collect(Collectors.toList());

        Map<String, Object> ret = Maps.newHashMap();
        ret.put("execution_ids", executionIds);
        ret.put("type", type);
        ret.put("cursor_time", time);
        ret.put("current_page", page);
        ret.put("next_page_available", nextPageAvailable);
        return ret;
    }


    public Map<String, Object> getExecutionIdsForBg(String serviceId, DAGStatus dagStatus, String code, Long time, Integer page, Integer count) {
        int offset = (page - 1) * count;
        List<Pair<String, String>> ids;
        String type;
        if (StringUtils.isBlank(code)) {
            ids = systemMonitorStatistic.getExecutionIdsByStatus(serviceId, dagStatus, time);
            type = dagStatus.getValue();
        } else {
            ids = systemMonitorStatistic.getExecutionIdsByCode(serviceId, code, time, offset, count + 1);
            type = code;
        }

        List<Map<String, Object>> executionIds = ids.stream()
                .map(it -> Map.of("execution_id", (Object) it.getLeft(), "submit_time", Long.valueOf(it.getRight())))
                .collect(Collectors.toList());

        Map<String, Object> ret = Maps.newHashMap();
        ret.put("execution_ids", executionIds);
        ret.put("type", type);
        ret.put("cursor_time", time);
        ret.put("current_page", page);
        return ret;
    }

    public Map<String, Object> statusCheck(String descriptorId, ResourceCheckConfig resourceCheckConfig) {
        String[] fields = StringUtils.isEmpty(descriptorId) ? new String[0] : descriptorId.trim().split(ReservedConstant.COLON);
        if (fields.length < 2 || StringUtils.isBlank(fields[0]) || StringUtils.isBlank(fields[1])) {
            log.info("statusCheck statusCheck:{} data format error", descriptorId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT.getCode(), "descriptorId:" + descriptorId + " format error");
        }
        String businessId = fields[0];
        String featureName = fields[1];
        String serviceId = businessId + ReservedConstant.COLON + featureName;

        Map<String, Object> submitStatus = dagSubmitChecker.getCheckRet(businessId, serviceId, resourceCheckConfig);

        return ImmutableMap.of("descriptor_id", descriptorId,
                "submit_status", submitStatus,
                "related_resources", dagResourceStatistic.orderDependentResources(serviceId));
    }

    public Map<String, Object> initBucket(String bucketName, JSONObject fieldToValues) {
        return ImmutableMap.of("bucket_name", customizedStorage.initBucket(bucketName, fieldToValues));
    }

    public Map<String, Object> storeAndNotify(String bucketName, JSONObject fieldToValues, String notifyTaskName, String notifyData) {
        customizedStorage.store(bucketName, fieldToValues);
        notifyTask(bucketName, notifyTaskName, notifyData);
        return ImmutableMap.of("ret", "ok");
    }

    private void notifyTask(String bucketName, String notifyTaskName, String notifyData) {
        if (StringUtils.isBlank(notifyTaskName)) {
            return;
        }

        JSONObject data = Optional.ofNullable(notifyData).map(JSONObject::parseObject).orElse(new JSONObject());
        String executionId = ExecutionIdUtil.getExecutionIdFromBucketName(bucketName);
        TaskInfo taskInfo = runtimeStorage.getTaskInfo(executionId, notifyTaskName);
        String taskCategory = taskInfo.getTask().getCategory();
        if (Objects.equals(taskCategory, TaskCategory.FUNCTION.getValue())) {
            finish(executionId, data.getJSONObject("response"), data);
        } else if (Objects.equals(taskCategory, TaskCategory.SUSPENSE.getValue())) {
            wakeup(executionId, notifyTaskName, data);
        } else {
            log.info("notifyTask do not support task type, bucketName:{}, taskName:{}, taskType:{}", bucketName, notifyTaskName, taskCategory);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION.getCode(), String.format("task type %s do not support", taskCategory));
        }
    }

    public Map<String, Object> load(String bucketName, boolean hGetAll, List<String> fieldNames, String fieldPrefix) {
        return ImmutableMap.of("bucket_name", bucketName,
                "field_to_value", customizedStorage.load(bucketName, hGetAll, fieldNames, fieldPrefix));
    }

    public Map<String, Object> remove(String bucketName) {
        return ImmutableMap.of("ret", customizedStorage.remove(bucketName));
    }

    public Map<String, Object> remove(String bucketName, List<String> fieldNames) {
        return ImmutableMap.of("ret", customizedStorage.remove(bucketName, fieldNames));
    }
}
