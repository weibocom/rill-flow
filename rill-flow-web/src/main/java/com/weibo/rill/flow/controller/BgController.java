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
import com.google.common.collect.ImmutableMap;
import com.weibo.rill.flow.common.model.DAGRecord;
import com.weibo.rill.flow.common.model.UserLoginRequest;
import com.weibo.rill.flow.service.facade.DAGDescriptorFacade;
import com.weibo.rill.flow.service.facade.DAGRuntimeFacade;
import com.weibo.rill.flow.service.storage.dao.DAGAliasDAO;
import com.weibo.rill.flow.service.storage.dao.DAGBusinessDAO;
import com.weibo.rill.flow.service.storage.dao.DAGFeatureDAO;
import com.weibo.rill.flow.service.trace.TraceableContextWrapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fenglin
 */
@Slf4j
@RestController
@Api(tags = {"管理后台接口"})
@RequestMapping("/flow/bg")
public class BgController {
    private static final String BUSINESS_IDS = "business_ids";

    @Value("${rill_flow_trace_query_host:}")
    private String traceQueryHost;

    @Autowired
    private DAGDescriptorFacade dagDescriptorFacade;

    @Autowired
    private DAGRuntimeFacade dagRuntimeFacade;

    @Autowired
    private DAGBusinessDAO dagBusinessDAO;

    @Autowired
    private DAGFeatureDAO dagFeatureDAO;

    @Autowired
    private DAGAliasDAO dagAliasDAO;

    /**
     * 流程图的详情
     *
     * @param descriptorId
     * @return
     */
    @ApiOperation(value = "获取流程图详情")
    @GetMapping(value = "/get_descriptor.json")
    public Map<String, Object> getDescriptor(
            HttpServletResponse response,
            @ApiParam(value = "流程图ID") @RequestParam(value = "id") String descriptorId
            ) {
        JSONObject descriptor = dagDescriptorFacade.getDescriptor(descriptorId);
        return Map.of("data", descriptor, "message", "", "success", true);
    }

    /**
     * 流程管理列表
     *
     * @param current
     * @param pageSize
     * @return
     */
    @ApiOperation(value = "流程管理列表")
    @GetMapping(value = "/get_descriptor_ids.json")
    public Map<String, Object> getDescriptorIds(
            @ApiParam(value = "页码") @RequestParam(value = "current") int current,
            @ApiParam(value = "每页条数") @RequestParam(value = "pageSize") int pageSize
    ) {
        JSONObject result = new JSONObject();
        List<DAGRecord> dagRecordList = new ArrayList<>();
        dagBusinessDAO.getBusiness().stream().forEach(bussinessId -> {
            dagFeatureDAO.getFeature(bussinessId).stream().forEach(featureId -> {
                dagAliasDAO.getAlias(bussinessId, featureId).stream().forEach(alia -> {
                    List<Map> versions = dagAliasDAO.getVersion(bussinessId, featureId, alia);
                    if (CollectionUtils.isNotEmpty(versions)) {
                        DAGRecord record = DAGRecord.builder()
                                .businessId(bussinessId)
                                .featureId(featureId)
                                .alias(alia)
                                .descriptorId(String.valueOf(versions.get(0).get("descriptor_id")))
                                .createTime(Long.parseLong(String.valueOf(versions.get(versions.size()-1).get("create_time"))))
                                .updateTime(Long.parseLong(String.valueOf(versions.get(0).get("create_time"))))
                                .build();
                        dagRecordList.add(record);
                    }
                });
            });
        });

        dagRecordList.sort((a, b) -> b.getUpdateTime().compareTo(a.getUpdateTime()));

        log.info("record curr:{}, pageSize:{}, result:{}", current, pageSize, result.toJSONString());
        return Map.of("items", dagRecordList, "total", dagRecordList.size());
    }

    /**
     * 执行详情列表页 查询接口
     *
     * @param business 业务名称
     * @param feature  服务名称
     * @param status   执行状态
     * @param code
     * @return
     */
    @ApiOperation(value = "执行详情列表页")
    @GetMapping(value = "get_execution_ids.json")
    public Map<String, Object> getExecutionIds(
            @ApiParam(value = "执行ID") @RequestParam(value = "id", required = false) String executionId,
            @ApiParam(value = "业务名称") @RequestParam(value = "business", required = false) String business,
            @ApiParam(value = "服务名称") @RequestParam(value = "feature", required = false) String feature,
            @ApiParam(value = "状态") @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "code", required = false) String code,
            @ApiParam(value = "开始时间") @RequestParam(value = "startTime", required = false, defaultValue = "0") Long startTime,
            @ApiParam(value = "结束时间") @RequestParam(value = "endTime", required = false) Long endTime,
            @ApiParam(value = "页码") @RequestParam(value = "page", required = false, defaultValue = "1") Integer current,
            @ApiParam(value = "每页条数") @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize
    ) {
        return dagRuntimeFacade.getExecutionIds(executionId, business, feature, status, code, startTime, endTime, current, pageSize);
    }

    /**
     * 根据executionId查询执行详情
     *
     * @param executionId
     * @return
     */
    @GetMapping(value = "/get_execution.json")
    public Map<String, Object> getExecution(
            @RequestParam(value = "id") String executionId
    ) {

        Map<String, Object> result = dagRuntimeFacade.getBasicDAGInfo(executionId, false);
        appendTraceInfo(result);
        return Map.of("data", result, "message", "", "success", true);
    }

    private void appendTraceInfo(Map<String, Object> result) {
        try {
            if (StringUtils.isBlank(traceQueryHost)) {
                return;
            }
            Map<String, Object> context = (Map<String, Object>) result.get("context");
            String traceId = new TraceableContextWrapper(context).getTraceId();
            result.put("trace_url", traceQueryHost + "/trace/" + traceId);
        } catch (Exception e) {
            log.warn("append Trace Info error, original result:{}", result);
        }
    }


    @PostMapping(value = "/user/login.json")
    public Map<String, Object> convert(@RequestBody UserLoginRequest request) {
        log.info("userName:{}, password:{}", request.getUsername(), request.getPassword());
        String user = "{\n" +
                "\t\"realName\": \"admin\",\n" +
                "\t\"roles\": [{\n" +
                "\t\t\"roleName\": \"Super Admin\",\n" +
                "\t\t\"value\": \"super\"\n" +
                "\t}],\n" +
                "\t\"userId\": \"1\",\n" +
                "\t\"username\": \"admin\",\n" +
                "\t\"token\": \"fakeToken1\",\n" +
                "\t\"desc\": \"manager\"\n" +
                "}";
        return JSON.parseObject(user);
    }

    @GetMapping(value = "/user/currentUser.json")
    public Map<String, Object> currentUser() {
        String user = "{\n" +
                "\t\"realName\": \"admin\",\n" +
                "\t\"password\": \"123456\",\n" +
                "\t\"homePath\": \"/flow-definition/list\",\n" +
                "\t\"roles\": [{\n" +
                "\t\t\"roleName\": \"Super Admin\",\n" +
                "\t\t\"value\": \"super\"\n" +
                "\t}],\n" +
                "\t\"avatar\": \"\",\n" +
                "\t\"userId\": \"1\",\n" +
                "\t\"username\": \"admin\",\n" +
                "\t\"desc\": \"manager\",\n" +
                "\t\"token\": \"fakeToken1\"\n" +
                "}";
        return JSON.parseObject(user);
    }


    @RequestMapping(value = "get_business_options.json", method = RequestMethod.GET)
    public Map<String, Object> getBusinessOptions() {
        @SuppressWarnings("unchecked")
        Set<String> businessIds = (Set<String>) dagDescriptorFacade.getBusiness().get(BUSINESS_IDS);
        return ImmutableMap.of(BUSINESS_IDS, businessIds.stream().map(item -> ImmutableMap.of("id", item, "name", item)).collect(Collectors.toList()));
    }
}
