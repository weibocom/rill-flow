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

package com.weibo.rill.flow.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.function.ResourceCheckConfig;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.context.DAGContextInitializer;
import com.weibo.rill.flow.service.facade.OlympiceneFacade;
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker;
import com.weibo.rill.flow.service.statistic.ProfileRecordService;
import com.weibo.rill.flow.service.util.DescriptorIdUtil;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

@RestController
@Api(tags = {"工作流操作相关接口"})
@RequestMapping("/flow")
public class FlowController {
    private static final String EXECUTION_ID = "execution_id";
    private static final String TASK_NAME = "task_name";
    private static final String TASK_NAMES = "task_names";
    private static final String SERVICE_ID = "service_id";
    private static final String TYPE = "type";
    private static final String CODE = "code";

    @Autowired
    private OlympiceneFacade olympiceneFacade;

    @Autowired
    private ProfileRecordService profileRecordService;

    @Autowired
    private DAGSubmitChecker submitChecker;

    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;

    @Autowired
    private DAGContextInitializer dagContextInitializer;

    /**
     * 任务提交接口
     *
     * @param flowUser      用户身份认证后的用户信息对象
     * @param descriptorId  DAG 图 ID
     * @param callback      非必须，执行完成后的回调地址
     * @param resourceCheck 用于检测资源是否可用的检测规则
     * @param data          图执行的 context 信息
     * @return
     */
    @ApiOperation(value = "执行工作流")
    @RequestMapping(value = "submit.json", method = RequestMethod.POST)
    public Map<String, Object> submit(User flowUser,
                                      @ApiParam(value = "工作流ID") @RequestParam(value = "descriptor_id") String descriptorId,
                                      @ApiParam(value = "执行完成后的回调地址") @RequestParam(value = "callback", required = false) String callback,
                                      @ApiParam(value = "用于检测资源是否可用的检测规则") @RequestParam(value = "resource_check", required = false) String resourceCheck,
                                      @ApiParam(value = "工作流执行的context信息") @RequestBody(required = false) JSONObject data) {
        Supplier<Map<String, Object>> submitActions = () -> {
            ResourceCheckConfig resourceCheckConfig = submitChecker.getCheckConfig(resourceCheck);
            String businessId = DescriptorIdUtil.changeDescriptorIdToBusinessId(descriptorId);
            Map<String, Object> context = dagContextInitializer.newSubmitContextBuilder(businessId).withData(data).withIdentity(descriptorId).build();

            return olympiceneFacade.submit(flowUser, descriptorId, context, callback, resourceCheckConfig);
        };

        return profileRecordService.runNotifyAndRecordProfile("submit.json", descriptorId, submitActions);
    }

    @ApiOperation(value = "任务完成回调")
    @RequestMapping(value = "finish.json", method = RequestMethod.POST)
    public Map<String, Object> finish(User flowUser,
                                      @ApiParam(value = "执行ID") @RequestParam(EXECUTION_ID) String executionId,
                                      @ApiParam(value = "任务名称") @RequestParam(TASK_NAME) String taskName,
                                      @ApiParam(value = "工作流执行的context信息") @RequestBody JSONObject result) {
        Supplier<Map<String, Object>> finishActions = () -> {
            String businessId = ExecutionIdUtil.getBusinessId(executionId);
            Map<String, Object> context = dagContextInitializer.newCallbackContextBuilder(businessId).withData(result).withIdentity(executionId).build();
            JSONObject data = new JSONObject();
            data.put("response", result);
            data.put("result_type", result.getOrDefault("result_type", "SUCCESS"));
            JSONObject passThrough = new JSONObject();
            passThrough.put(EXECUTION_ID, executionId);
            passThrough.put(TASK_NAME, taskName);
            data.put("passthrough", passThrough);
            return olympiceneFacade.finish(executionId, context, data);
        };
        return profileRecordService.runNotifyAndRecordProfile("finish.json", executionId, finishActions);
    }

    @ApiOperation(value = "唤醒挂起任务")
    @RequestMapping(value = "wakeup.json", method = RequestMethod.POST)
    public Map<String, Object> wakeup(User flowUser,
                                      @ApiParam(value = "执行ID") @RequestParam(value = EXECUTION_ID) String executionId,
                                      @ApiParam(value = "任务名称") @RequestParam(value = TASK_NAME) String taskName,
                                      @ApiParam(value = "工作流执行的context信息") @RequestBody(required = false) JSONObject data) {
        Supplier<Map<String, Object>> wakeupActions = () -> {
            if (StringUtils.isEmpty(taskName)) {
                throw new TaskException(BizError.ERROR_DATA_FORMAT, executionId, "task_name is empty");
            }

            String businessId = ExecutionIdUtil.getBusinessId(executionId);
            Map<String, Object> context = dagContextInitializer.newWakeupContextBuilder(businessId).withData(data).withIdentity(executionId).build();
            return olympiceneFacade.wakeup(executionId, taskName, context);
        };

        return profileRecordService.runNotifyAndRecordProfile("wakeup.json", executionId, wakeupActions);
    }

    @ApiOperation(value = "任务重做")
    @RequestMapping(value = "redo.json", method = RequestMethod.POST)
    public Map<String, Object> redo(User flowUser,
                                    @ApiParam(value = "执行ID") @RequestParam(value = EXECUTION_ID) String executionId,
                                    @ApiParam(value = "任务名称列表") @RequestParam(value = TASK_NAMES, required = false) List<String> taskNames,
                                    @ApiParam(value = "工作流执行的context信息") @RequestBody(required = false) JSONObject data) {
        Supplier<Map<String, Object>> redoActions = () -> {
            String businessId = ExecutionIdUtil.getBusinessId(executionId);
            Map<String, Object> context = dagContextInitializer.newRedoContextBuilder(businessId).withData(data).withIdentity(executionId).build();
            return olympiceneFacade.redo(executionId, taskNames, context);
        };

        return profileRecordService.runNotifyAndRecordProfile("redo.json", executionId, redoActions);
    }

    @RequestMapping(value = "multi_redo.json", method = RequestMethod.POST)
    public Map<String, Object> multiRedo(User flowUser,
                                         @RequestParam(value = SERVICE_ID) String serviceId,
                                         @RequestParam(value = TYPE, defaultValue = "failed") String type,
                                         @RequestParam(value = CODE, required = false) String code,
                                         @RequestParam(value = "offset_in_minute", defaultValue = "0") Integer offsetInMinute,
                                         @RequestParam(value = "count", defaultValue = "100") Integer count,
                                         @RequestParam(value = TASK_NAMES, required = false) List<String> taskNames,
                                         @RequestParam(value = "rate_limit", defaultValue = "30") Integer rate) {
        DAGStatus dagStatus = DAGStatus.parse(type);
        if (dagStatus != DAGStatus.RUNNING && dagStatus != DAGStatus.FAILED) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, String.format("type %s nonsupport", type));
        }

        if (rate <= 0 || rate > 50) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, String.format("rate_limit %s nonsupport", rate));
        }

        if (count <= 0 || count > 30000) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, String.format("count %s nonsupport", count));
        }

        long cursor = System.currentTimeMillis() - offsetInMinute * 60 * 1000;

        olympiceneFacade.multiRedo(serviceId, dagStatus, code, cursor, count, taskNames, rate);

        return Map.of("result", "ok");
    }

    @ApiOperation(value = "任务降级")
    @RequestMapping(value = "task_degrade.json", method = RequestMethod.POST)
    public Map<String, Object> taskDegrade(User flowUser,
                                           @ApiParam(value = "执行ID") @RequestParam(value = EXECUTION_ID) String executionId,
                                           @ApiParam(value = "任务名称") @RequestParam(value = TASK_NAME) String taskName,
                                           @ApiParam(value = "降级当前节点 true: 降级 false: 不降级") @RequestParam(value = "degrade_current_task") boolean degradeCurrentTask,
                                           @ApiParam(value = "降级当前节点后续节点 true: 降级 false: 不降级") @RequestParam(value = "degrade_following_tasks") boolean degradeFollowingTasks) {
        Supplier<Map<String, Object>> redoActions = () -> olympiceneFacade.taskDegrade(executionId, taskName, degradeCurrentTask, degradeFollowingTasks);

        return profileRecordService.runNotifyAndRecordProfile("task_degrade.json", executionId, redoActions);
    }

    @ApiOperation(value = "业务统计成功率统计信息")
    @RequestMapping(value = "business_heap_monitor.json", method = RequestMethod.GET)
    public JSONObject businessHeapMonitor(User flowUser,
                                          @ApiParam(value = "服务id,多个以英文逗号间隔 serviceId格式businessId:feature") @RequestParam(value = "service_ids", required = false) List<String> serviceIds,
                                          @ApiParam(value = "统计时间段起始时间偏移量 单位:分钟") @RequestParam(value = "start_time_offset", required = false) Integer startTimeOffset,
                                          @ApiParam(value = "统计时间段结束时间偏移量 单位:分钟") @RequestParam(value = "end_time_offset", required = false) Integer endTimeOffset) {
        List<Integer> timeOffset = Lists.newArrayList(startTimeOffset, endTimeOffset);
        if (timeOffset.stream().filter(Objects::nonNull).count() == 1) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "time offset should be all configured");
        }
        if (startTimeOffset != null && endTimeOffset != null && startTimeOffset <= endTimeOffset) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "start time offset should be bigger than end time offset");
        }
        return olympiceneFacade.businessHeapMonitor(serviceIds, startTimeOffset, endTimeOffset);
    }

    @ApiOperation(value = "获取dag流程执行数量")
    @RequestMapping(value = "get_execution_count.json", method = RequestMethod.GET)
    public Map<String, Object> getExecutionCount(User flowUser,
                                                 @ApiParam(value = "服务ID 格式businessId:feature") @RequestParam(value = SERVICE_ID) String serviceId,
                                                 @ApiParam(value = "dag流程状态，取值: succeed/running/failed") @RequestParam(value = TYPE, required = false) String type,
                                                 @ApiParam(value = "任务失败code") @RequestParam(value = CODE, required = false) String code,
                                                 @ApiParam(value = "统计时间段起始时间偏移量 单位: 分钟") @RequestParam(value = "start_time_offset", required = false) Integer startTimeOffset,
                                                 @ApiParam(value = "统计时间段结束时间偏移量 单位: 分钟") @RequestParam(value = "end_time_offset", required = false) Integer endTimeOffset) {
        DAGStatus dagStatus = Optional.ofNullable(type).map(DAGStatus::parse).orElse(null);
        return olympiceneFacade.getExecutionCount(serviceId, dagStatus, code, startTimeOffset, endTimeOffset);
    }

    @ApiOperation(value = "获取业务dag流程的执行id")
    @RequestMapping(value = "get_execution_ids.json", method = RequestMethod.GET)
    public Map<String, Object> getExecutionIds(User flowUser,
                                               @ApiParam(value = "服务ID 格式 businessId:feature") @RequestParam(value = SERVICE_ID) String serviceId,
                                               @ApiParam(value = "dag流程状态，取值: succeed/running/failed") @RequestParam(value = TYPE, defaultValue = "succeed") String type,
                                               @ApiParam(value = "任务失败code") @RequestParam(value = CODE, required = false) String code,
                                               @ApiParam(value = "时间毫秒数") @RequestParam(value = "time", required = false) Long time,
                                               @ApiParam(value = "当前页码") @RequestParam(value = "page", required = false, defaultValue = "1") Integer page,
                                               @ApiParam(value = "当前页的条数") @RequestParam(value = "count", required = false, defaultValue = "20") Integer count) {
        DAGStatus dagStatus = Optional.ofNullable(DAGStatus.parse(type))
                .orElseThrow(() -> new TaskException(BizError.ERROR_DATA_FORMAT, "type value nonsupport"));
        long endTime = Optional.ofNullable(time).orElse(System.currentTimeMillis());
        if (count > 100) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "count too big");
        }

        return olympiceneFacade.getExecutionIds(serviceId, dagStatus, code, endTime, page, count);
    }

    @RequestMapping(value = "status/check.json", method = RequestMethod.GET)
    public Map<String, Object> statusCheck(User flowUser,
                                           @RequestParam(value = "descriptor_id") String descriptorId,
                                           @RequestParam(value = "resource_check", required = false) String resourceCheck) {
        ResourceCheckConfig resourceCheckConfig = submitChecker.getCheckConfig(resourceCheck);
        return olympiceneFacade.statusCheck(descriptorId, resourceCheckConfig);
    }

    @RequestMapping(value = "customized/storage/init_bucket.json", method = RequestMethod.POST)
    public Map<String, Object> initBucket(User flowUser,
                                          @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                          @RequestParam(value = "business_id", required = false) String businessId,
                                          @RequestParam(value = "feature_name", defaultValue = "placeholder") String featureName,
                                          @RequestBody(required = false) JSONObject fieldToValues) {
        if (StringUtils.isAllBlank(executionId, businessId)) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "id can not be empty");
        }

        String bucketExecutionId = Optional.ofNullable(executionId)
                .filter(StringUtils::isNotBlank)
                .orElse(ExecutionIdUtil.generateExecutionId(businessId, featureName));
        String bucketName = ExecutionIdUtil.generateBucketName(bucketExecutionId);
        Supplier<Map<String, Object>> initActions = () ->
                olympiceneFacade.initBucket(bucketName, JSON.parseObject(fieldToValues.toJSONString()));

        return profileRecordService.runNotifyAndRecordProfile("customized/storage/init_bucket.json", bucketName, initActions);
    }

    @RequestMapping(value = "customized/storage/store.json", method = RequestMethod.POST)
    public Map<String, Object> store(User flowUser,
                                     @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                     @RequestParam(value = "bucket_name", required = false) String bucketName,
                                     @RequestParam(value = "notify_task_name", required = false) String notifyTaskName,
                                     @RequestParam(value = "notify_data", required = false) String notifyData,
                                     @RequestBody(required = false) JSONObject fieldToValues) {
        String bkName = getBucketName(executionId, bucketName);
        Supplier<Map<String, Object>> storeActions = () ->
                olympiceneFacade.storeAndNotify(bkName, JSON.parseObject(fieldToValues.toJSONString()), notifyTaskName, notifyData);
        return profileRecordService.runNotifyAndRecordProfile("customized/storage/store.json", bkName, storeActions);
    }

    @RequestMapping(value = "customized/storage/load.json", method = RequestMethod.GET)
    public Map<String, Object> load(User flowUser,
                                    @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                    @RequestParam(value = "bucket_name", required = false) String bucketName,
                                    @RequestParam(value = "all_element", defaultValue = "false") boolean hGetAll,
                                    @RequestParam(value = "field_names", required = false) List<String> fieldNames,
                                    @RequestParam(value = "field_prefix", required = false) String fieldPrefix) {
        String bkName = getBucketName(executionId, bucketName);
        Supplier<Map<String, Object>> loadActions = () -> olympiceneFacade.load(bkName, hGetAll, fieldNames, fieldPrefix);
        return profileRecordService.runNotifyAndRecordProfile("customized/storage/load.json", bkName, loadActions);
    }

    @RequestMapping(value = "customized/storage/remove_bucket.json", method = RequestMethod.POST)
    public Map<String, Object> removeBucket(User flowUser,
                                            @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                            @RequestParam(value = "bucket_name", required = false) String bucketName) {
        String bkName = getBucketName(executionId, bucketName);
        Supplier<Map<String, Object>> removeBucketActions = () -> olympiceneFacade.remove(bkName);
        return profileRecordService.runNotifyAndRecordProfile("customized/storage/remove_bucket.json", bkName, removeBucketActions);
    }

    @RequestMapping(value = "customized/storage/remove_element.json", method = RequestMethod.POST)
    public Map<String, Object> removeElement(User flowUser,
                                             @RequestParam(value = EXECUTION_ID, required = false) String executionId,
                                             @RequestParam(value = "bucket_name", required = false) String bucketName,
                                             @RequestParam(value = "field_names") List<String> fieldNames) {
        String bkName = getBucketName(executionId, bucketName);
        Supplier<Map<String, Object>> removeElementActions = () -> olympiceneFacade.remove(bkName, fieldNames);
        return profileRecordService.runNotifyAndRecordProfile("customized/storage/remove_element.json", bkName, removeElementActions);
    }

    private String getBucketName(String executionId, String bucketName) {
        if (StringUtils.isAllBlank(executionId, bucketName)) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "execution_id and bucket_name can not be all empty");
        }
        return Optional.ofNullable(bucketName)
                .filter(StringUtils::isNotBlank)
                .orElse(ExecutionIdUtil.generateBucketName(executionId));
    }

    @GetMapping(value = "/dag/details.json")
    public Map<String, Object> getDagDetails(
            @RequestParam(value = "id") String id,
            @RequestParam(value = "status") String status
    ) {
        String result = redisClient.get(id);
        if (StringUtils.isEmpty(result)) {
            return Map.of("data", "", "message", "", "success", true);
        }
        return Map.of("data", JSONObject.parseObject(result), "message", "", "success", true);
    }


}
