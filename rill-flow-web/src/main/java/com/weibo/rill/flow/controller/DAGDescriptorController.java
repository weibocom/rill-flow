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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.service.facade.DAGDescriptorFacade;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

@RestController
@Api(tags = {"工作流描述相关接口"})
@RequestMapping("/flow/bg/manage/descriptor")
public class DAGDescriptorController {
    private static final Set<String> FUNCTION_AB_PROTOCOLS = Sets.newHashSet("rillflow", "http", "https", "resourceRef");

    @Autowired
    private DAGDescriptorFacade dagDescriptorFacade;

    @ApiOperation(value = "业务名称管理")
    @RequestMapping(value = "modify_business.json", method = RequestMethod.POST)
    public Map<String, Object> modifyBusiness(User flowUser,
                                              @ApiParam(value = "增加:true, 删除:false") @RequestParam(value = "add") boolean add,
                                              @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId) {
        return dagDescriptorFacade.modifyBusiness(add, businessId);
    }

    @ApiOperation(value = "业务名称获取")
    @RequestMapping(value = "get_business.json", method = RequestMethod.GET)
    public Map<String, Object> getBusiness(User flowUser) {
        return dagDescriptorFacade.getBusiness();
    }

    @ApiOperation(value = "业务功能管理")
    @RequestMapping(value = "modify_feature.json", method = RequestMethod.POST)
    public Map<String, Object> modifyFeature(User flowUser,
                                             @ApiParam(value = "增加:true, 删除:false") @RequestParam(value = "add") boolean add,
                                             @ApiParam(value = "业务id") @RequestParam(value = "business_id") String businessId,
                                             @ApiParam(value = "服务名称") @RequestParam(value = "feature_name") String featureName) {
        return dagDescriptorFacade.modifyFeature(add, businessId, featureName);
    }

    @ApiOperation(value = "获取业务功能")
    @RequestMapping(value = "get_feature.json", method = RequestMethod.GET)
    public Map<String, Object> getFeature(User flowUser,
                                          @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId) {
        return dagDescriptorFacade.getFeature(businessId);
    }

    @ApiOperation(value = "功能别名管理")
    @RequestMapping(value = "modify_alias.json", method = RequestMethod.POST)
    public Map<String, Object> modifyAlias(User flowUser,
                                           @ApiParam(value = "增加:true, 删除:false") @RequestParam(value = "add") boolean add,
                                           @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                           @ApiParam(value = "服务名称") @RequestParam(value = "feature_name") String featureName,
                                           @ApiParam(value = "别名") @RequestParam(value = "alias") String alias) {
        return dagDescriptorFacade.modifyAlias(add, businessId, featureName, alias);
    }

    @ApiOperation(value = "获取别名")
    @RequestMapping(value = "get_alias.json", method = RequestMethod.GET)
    public Map<String, Object> getAlias(User flowUser,
                                        @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                        @ApiParam(value = "服务名称") @RequestParam(value = "feature_name") String featureName) {
        return dagDescriptorFacade.getAlias(businessId, featureName);
    }

    @ApiOperation(value = "别名灰度管理")
    @RequestMapping(value = "modify_gray.json", method = RequestMethod.POST)
    public Map<String, Object> modifyGray(User flowUser,
                                          @RequestParam(value = "identity", required = false) String identity,
                                          @ApiParam(value = "增加:true, 删除:false") @RequestParam(value = "add") boolean add,
                                          @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                          @ApiParam(value = "服务名称") @RequestParam(value = "feature_name") String featureName,
                                          @ApiParam(value = "别名") @RequestParam(value = "alias") String alias,
                                          @ApiParam(value = "alias对应的灰度策略 aviator表达式 如：rand(100) < 10 add为true时必传") @RequestParam(value = "gray_rule", required = false) String grayRule) {
        if (add && StringUtils.isBlank(grayRule)) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT.getCode(), "gray_rule can not be empty when add gray");
        }

        return dagDescriptorFacade.modifyGray(identity, add, businessId, featureName, alias, grayRule);
    }

    @ApiOperation(value = "获取别名灰度")
    @RequestMapping(value = "get_gray.json", method = RequestMethod.GET)
    public Map<String, Object> getGray(User flowUser,
                                       @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                       @ApiParam(value = "服务名称") @RequestParam(value = "feature_name") String featureName) {
        return dagDescriptorFacade.getGray(businessId, featureName);
    }

    @ApiOperation(value = "获取资源灰度配置项")
    @RequestMapping(value = "get_ab_key.json", method = RequestMethod.GET)
    public Map<String, Object> getABConfigKey(User flowUser,
                                              @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId) {
        return dagDescriptorFacade.getABConfigKey(businessId);
    }

    @ApiOperation(value = "资源灰度管理")
    @RequestMapping(value = "modify_function_ab.json", method = RequestMethod.POST)
    public Map<String, Object> modifyFunctionAB(User flowUser,
                                                @RequestParam(value = "identity", required = false) String identity,
                                                @ApiParam(value = "增加:true, 删除:false") @RequestParam(value = "add") boolean add,
                                                @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                                @ApiParam(value = "资源配置名称，由大小写字母加数字组成") @RequestParam(value = "config_key") String configKey,
                                                @ApiParam(value = "任务资源名称") @RequestParam(value = "resource_name") String resourceName,
                                                @ApiParam(value = "资源对应的灰度策略 aviator表达式 如：rand(100) < 10 add为true时必传") @RequestParam(value = "ab_rule", required = false) String abRule) {
        if (add && StringUtils.isBlank(abRule)) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT.getCode(), "ab_rule can not be empty when add action");
        }

        if (StringUtils.isEmpty(resourceName) ||
                !FUNCTION_AB_PROTOCOLS.contains(resourceName.split(Resource.CONNECTOR)[0])) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT.getCode(), "resource_name format is not support");
        }

        return dagDescriptorFacade.modifyFunctionAB(identity, add, businessId, configKey, resourceName, abRule);
    }

    @ApiOperation(value = "获取函数资源配置项")
    @RequestMapping(value = "get_function_ab.json", method = RequestMethod.GET)
    public Map<String, Object> getFunctionAB(User flowUser,
                                             @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                             @ApiParam(value = "资源配置名称") @RequestParam(value = "config_key") String configKey) {
        return dagDescriptorFacade.getFunctionAB(businessId, configKey);
    }

    @ApiOperation(value = "获取别名内yaml版本")
    @RequestMapping(value = "get_version.json", method = RequestMethod.GET)
    public Map<String, Object> getVersion(User flowUser,
                                          @ApiParam(value = "业务ID") @RequestParam(value = "business_id") String businessId,
                                          @ApiParam(value = "服务名称") @RequestParam(value = "feature_name") String featureName,
                                          @ApiParam(value = "别名") @RequestParam(value = "alias") String alias) {
        return dagDescriptorFacade.getVersion(businessId, featureName, alias);
    }

    /**
     * 创建 DAG 图
     * @param flowUser 验证用户身份后的用户对象
     * @param identity 用于发送邮件时的发送人信息
     * @param businessId DAG 图的业务 ID
     * @param featureName DAG 图的图 ID
     * @param alias DAG 图的别名，用于别名灰度管理
     * @param descriptor 构成图的 yaml
     * @return
     */
    @ApiOperation(value = "创建工作流")
    @RequestMapping(value = "add_descriptor.json", method = RequestMethod.POST)
    public Map<String, Object> addDescriptor(User flowUser,
                                             @ApiParam(value = "用于发送邮件时的发送人信息") @RequestParam(value = "identity", required = false) String identity,
                                             @ApiParam(value = "工作流业务ID") @RequestParam(value = "business_id") String businessId,
                                             @ApiParam(value = "工作流服务名称") @RequestParam(value = "feature_name") String featureName,
                                             @ApiParam(value = "工作流别名") @RequestParam(value = "alias") String alias,
                                             @ApiParam(value = "工作流yaml") @RequestBody String descriptor) {
        return dagDescriptorFacade.addDescriptor(identity, businessId, featureName, alias, descriptor);
    }

    @ApiOperation(value = "获取工作流描述yaml文件")
    @RequestMapping(value = "get_descriptor.json", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> getDescriptor(User flowUser,
                                             @RequestParam(value = "uid", required = false) Long uid,
                                             @ApiParam(value = "工作流ID") @RequestParam(value = "descriptor_id") String descriptorId,
                                             @RequestBody(required = false) JSONObject data) {
        return dagDescriptorFacade.getDescriptor(uid, data, descriptorId);
    }
}
