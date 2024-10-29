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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO;
import com.weibo.rill.flow.olympicene.core.model.event.DAGDescriptorEvent;
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter;
import com.weibo.rill.flow.service.service.DAGDescriptorService;
import com.weibo.rill.flow.service.statistic.DAGSubmitChecker;
import com.weibo.rill.flow.service.storage.dao.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


@Slf4j
@Service
public class DAGDescriptorFacade {
    private static final String RET = "ret";
    private static final String BUSINESS_IDS = "business_ids";
    private static final String BUSINESS_ID = "business_id";
    private static final String FEATURES = "features";
    private static final String FEATURE = "feature";
    private static final String ALIASES = "aliases";
    private static final String ALIAS = "alias";
    private static final String GRAY = "gray";
    private static final String VERSIONS = "versions";
    private static final String DESCRIPTOR_ID = "descriptor_id";
    private static final String DESCRIPTOR = "descriptor";
    @Autowired
    private DAGDescriptorService dagDescriptorService;
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    @Autowired
    private DAGSubmitChecker dagSubmitChecker;
    @Autowired
    private DAGGrayDAO dagGrayDAO;
    @Autowired
    private DAGBusinessDAO dagBusinessDAO;
    @Autowired
    private DAGFeatureDAO dagFeatureDAO;
    @Autowired
    private DAGAliasDAO dagAliasDAO;
    @Autowired
    private DAGABTestDAO dagabTestDAO;
    @Autowired
    private DAGDescriptorConverter dagDescriptorConverter;

    public Map<String, Object> modifyBusiness(boolean add, String businessId) {
        boolean ret = add ? dagBusinessDAO.createBusiness(businessId) : dagBusinessDAO.remBusiness(businessId);
        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getBusiness() {
        return ImmutableMap.of(BUSINESS_IDS, dagBusinessDAO.getBusiness());
    }

    public Map<String, Object> modifyFeature(boolean add, String businessId, String featureName) {
        boolean ret;
        if (add) {
            dagBusinessDAO.createBusiness(businessId);
            ret = dagFeatureDAO.createFeature(businessId, featureName);
        } else {
            ret = dagFeatureDAO.remFeature(businessId, featureName);
        }
        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getFeature(String businessId) {
        return ImmutableMap.of(BUSINESS_ID, businessId, FEATURES, dagFeatureDAO.getFeature(businessId));
    }

    public Map<String, Object> modifyAlias(boolean add, String businessId, String featureName, String alias) {
        boolean ret;
        if (add) {
            dagBusinessDAO.createBusiness(businessId);
            dagFeatureDAO.createFeature(businessId, featureName);
            ret = dagAliasDAO.createAlias(businessId, featureName, alias);
        } else {
            ret = dagAliasDAO.remAlias(businessId, featureName, alias);
        }
        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getAlias(String businessId, String featureName) {
        return ImmutableMap.of(BUSINESS_ID, businessId,
                FEATURE, featureName,
                ALIASES, dagAliasDAO.getAlias(businessId, featureName));
    }

    public Map<String, Object> modifyGray(String identity, boolean add, String businessId, String featureName, String alias, String grayRule) {
        boolean ret;
        if (add) {
            dagBusinessDAO.createBusiness(businessId);
            dagFeatureDAO.createFeature(businessId, featureName);
            dagAliasDAO.createAlias(businessId, featureName, alias);
            ret = dagGrayDAO.createGray(businessId, featureName, alias, grayRule);
        } else {
            ret = dagGrayDAO.remGray(businessId, featureName, alias);
        }

        DAGDescriptorEvent.DAGDescriptorOperation operation = DAGDescriptorEvent.DAGDescriptorOperation.builder().add(add)
                .identity(identity)
                .businessId(businessId)
                .featureName(featureName)
                .alias(alias)
                .grayRule(grayRule)
                .type(DAGDescriptorEvent.Type.modifyGray)
                .build();
        applicationEventPublisher.publishEvent(new DAGDescriptorEvent(operation));

        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getGray(String businessId, String featureName) {
        return ImmutableMap.of(BUSINESS_ID, businessId,
                FEATURE, featureName,
                GRAY, dagGrayDAO.getGray(businessId, featureName));
    }

    public Map<String, Object> getABConfigKey(String businessId) {
        return ImmutableMap.of(BUSINESS_ID, businessId, "config_keys", dagabTestDAO.getABConfigKey(businessId));
    }

    public Map<String, Object> modifyFunctionAB(String identity, boolean add, String businessId, String configKey, String resourceName, String abRule) {
        boolean ret = add ? dagabTestDAO.createFunctionAB(businessId, configKey, resourceName, abRule) :
                dagabTestDAO.remFunctionAB(businessId, configKey, resourceName);

        DAGDescriptorEvent.DAGDescriptorOperation operation = DAGDescriptorEvent.DAGDescriptorOperation.builder().add(add)
                .identity(identity)
                .businessId(businessId)
                .configKey(configKey)
                .resourceName(resourceName)
                .abRule(abRule)
                .type(DAGDescriptorEvent.Type.modifyGray)
                .build();
        applicationEventPublisher.publishEvent(new DAGDescriptorEvent(operation));


        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getFunctionAB(String businessId, String configKey) {
        Pair<String, Map<String, String>> functionAB = dagabTestDAO.getFunctionAB(businessId, configKey);
        Map<String, Object> ab = Maps.newHashMap();
        ab.put("default_resource_name", functionAB.getLeft());
        ab.put("rules", functionAB.getRight().entrySet().stream()
                .map(entry -> ImmutableMap.of("resource_name", entry.getKey(), "rule", entry.getValue()))
                .collect(Collectors.toList())
        );
        return ImmutableMap.of(BUSINESS_ID, businessId, "config_key", configKey, "ab", ab);
    }

    public Map<String, Object> getVersion(String businessId, String featureName, String alias) {
        return ImmutableMap.of(
                BUSINESS_ID, businessId,
                FEATURE, featureName,
                ALIAS, alias,
                VERSIONS, dagAliasDAO.getVersion(businessId, featureName, alias));
    }

    public Map<String, Object> addDescriptor(String identity, String businessId, String featureName, String alias, String descriptor) {
        try {
            if (StringUtils.isNotEmpty(descriptor)) {
                dagSubmitChecker.checkDAGInfoLengthByBusinessId(businessId, List.of(descriptor.getBytes(StandardCharsets.UTF_8)));
            }
            DescriptorVO descriptorVO = new DescriptorVO(descriptor);
            String descriptorId = dagDescriptorService.saveDescriptorVO(businessId, featureName, alias, descriptorVO);

            Map<String, String> attachments = Maps.newHashMap();
            String attachmentName = String.format("descriptor-%s_%s_%s.txt", businessId, featureName, alias);
            attachments.put(attachmentName, descriptor);
            DAGDescriptorEvent.DAGDescriptorOperation operation = DAGDescriptorEvent.DAGDescriptorOperation.builder()
                    .identity(identity)
                    .businessId(businessId)
                    .featureName(featureName)
                    .alias(alias)
                    .attachments(attachments)
                    .type(DAGDescriptorEvent.Type.addDescriptor)
                    .add(false).build();
            applicationEventPublisher.publishEvent(new DAGDescriptorEvent(operation));

            return ImmutableMap.of(RET, descriptorId != null, DESCRIPTOR_ID, Optional.ofNullable(descriptorId).orElse(""));
        } catch (Exception e) {
            log.warn("addDescriptor fails, ", e);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, e.getMessage());
        }
    }

    public Map<String, Object> getDescriptor(Long uidOriginal, Map<String, Object> input, String descriptorId) {
        Long uid = Optional.ofNullable(uidOriginal).orElse(
                Optional.ofNullable(input)
                        .map(it -> it.get("uid"))
                        .map(it -> Long.parseLong(String.valueOf(it)))
                        .orElse(0L)
        );
        DAG dag = dagDescriptorService.getDAG(uid, input, descriptorId);
        DescriptorVO descriptorVO = dagDescriptorConverter.convertDAGToDescriptorVO(dag);
        return ImmutableMap.of(DESCRIPTOR_ID, descriptorId,
                "uid", String.valueOf(uid),
                DESCRIPTOR, descriptorVO.getDescriptor());
    }

    public JSONObject getDescriptor(String descriptorId) {
        DAG dag = dagDescriptorService.getDAG(null, null, descriptorId);
        DescriptorVO descriptorVO = dagDescriptorConverter.convertDAGToDescriptorVO(dag);
        JSONObject descriptorObject = yamlToJson(descriptorVO.getDescriptor());
        if (descriptorObject == null) {
            log.warn("descriptorId:{} descriptor is null", descriptorId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "descriptor is null: " + descriptorId);
        }
        //tasks
        JSONObject tasks = new JSONObject();
        JSONArray descriptorTasks = descriptorObject.getJSONArray("tasks");
        for (int i = 0; i < descriptorTasks.size(); i++) {
            JSONObject task = new JSONObject();
            JSONObject descriptorTask = descriptorTasks.getJSONObject(i);
            generateResourceProtocol(descriptorTask);
            task.put("task", descriptorTask);
            task.put("next", Optional.ofNullable(descriptorTask.getString("next")).map(it -> List.of(it.split(","))).orElse(Lists.newArrayList()));
            tasks.put(descriptorTask.getString("name"), task);
        }
        descriptorObject.put("tasks", tasks);
        return descriptorObject;
    }

    /**
     * 通过 task 生成 task.resourceProtocol
     * @param task 任务结构
     */
    private void generateResourceProtocol(JSONObject task) {
        try {
            if (task == null || StringUtils.isNotEmpty(task.getString(task.getString("resourceProtocol")))) {
                return;
            }
            String resourceName = task.getString("resourceName");
            Resource resource = new Resource(resourceName);
            task.put("resourceProtocol", resource.getSchemeProtocol());
        } catch (Exception e) {
            log.warn("generateResourceProtocol error", e);
        }
    }

    private JSONObject yamlToJson(String yamlString) {
        try {
            // Create an ObjectMapper for YAML
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

            // Convert YAML to a Java object
            Object yamlObject = yamlMapper.readValue(yamlString, Object.class);

            // Create a regular ObjectMapper for JSON
            ObjectMapper jsonMapper = new ObjectMapper();

            // Convert the Java object to JSON
            String descriptor = jsonMapper.writeValueAsString(yamlObject);

            return JSON.parseObject(descriptor);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

}
