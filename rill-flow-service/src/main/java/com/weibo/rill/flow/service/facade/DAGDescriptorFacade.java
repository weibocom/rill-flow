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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.Node;
import com.weibo.rill.flow.common.model.NodeType;
import com.weibo.rill.flow.olympicene.core.model.event.DAGDescriptorEvent;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.manager.DescriptorManager;
import com.weibo.rill.flow.service.service.ProtocolPluginService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.*;
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
    ProtocolPluginService protocolPluginService;
    @Autowired
    private DescriptorManager descriptorManager;
    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    public Map<String, Object> modifyBusiness(boolean add, String businessId) {
        boolean ret = add ? descriptorManager.createBusiness(businessId) : descriptorManager.remBusiness(businessId);
        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getBusiness() {
        return ImmutableMap.of(BUSINESS_IDS, descriptorManager.getBusiness());
    }

    public Map<String, Object> modifyFeature(boolean add, String businessId, String featureName) {
        boolean ret = add ?
                descriptorManager.createFeature(businessId, featureName) : descriptorManager.remFeature(businessId, featureName);
        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getFeature(String businessId) {
        return ImmutableMap.of(BUSINESS_ID, businessId, FEATURES, descriptorManager.getFeature(businessId));
    }

    public Map<String, Object> modifyAlias(boolean add, String businessId, String featureName, String alias) {
        boolean ret = add ?
                descriptorManager.createAlias(businessId, featureName, alias) : descriptorManager.remAlias(businessId, featureName, alias);
        return ImmutableMap.of(RET, ret);
    }

    public Map<String, Object> getAlias(String businessId, String featureName) {
        return ImmutableMap.of(BUSINESS_ID, businessId,
                FEATURE, featureName,
                ALIASES, descriptorManager.getAlias(businessId, featureName));
    }

    public Map<String, Object> modifyGray(String identity, boolean add, String businessId, String featureName, String alias, String grayRule) {
        boolean ret = add ?
                descriptorManager.createGray(businessId, featureName, alias, grayRule) :
                descriptorManager.remGray(businessId, featureName, alias);

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
                GRAY, descriptorManager.getGray(businessId, featureName));
    }

    public Map<String, Object> getABConfigKey(String businessId) {
        return ImmutableMap.of(BUSINESS_ID, businessId, "config_keys", descriptorManager.getABConfigKey(businessId));
    }

    public Map<String, Object> modifyFunctionAB(String identity, boolean add, String businessId, String configKey, String resourceName, String abRule) {
        boolean ret = add ? descriptorManager.createFunctionAB(businessId, configKey, resourceName, abRule) :
                descriptorManager.remFunctionAB(businessId, configKey, resourceName);

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
        Pair<String, Map<String, String>> functionAB = descriptorManager.getFunctionAB(businessId, configKey);
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
                VERSIONS, descriptorManager.getVersion(businessId, featureName, alias));
    }

    public Map<String, Object> addDescriptor(String identity, String businessId, String featureName, String alias, String descriptor) {
        try {
            String descriptorId = descriptorManager.createDAGDescriptor(businessId, featureName, alias, descriptor);

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
                    .build();
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
        return ImmutableMap.of(DESCRIPTOR_ID, descriptorId,
                "uid", String.valueOf(uid),
                DESCRIPTOR, descriptorManager.getDagDescriptor(uid, input, descriptorId));
    }

    public JSONObject getDescriptor(String descriptorId) {
        String descriptor = descriptorManager.getDagDescriptor(null, null, descriptorId);
        JSONObject descriptorObject = yamlToJson(descriptor);
        //tasks
        JSONObject tasks = new JSONObject();
        JSONArray descriptorTasks = descriptorObject.getJSONArray("tasks");
        for (int i = 0; i < descriptorTasks.size(); i++) {
            JSONObject task = new JSONObject();
            JSONObject descriptorTask = descriptorTasks.getJSONObject(i);
            task.put("task", descriptorTask);
            task.put("next", Optional.ofNullable(descriptorTask.getString("next")).map(it -> List.of(it.split(","))).orElse(Lists.newArrayList()));
            tasks.put(descriptorTask.getString("name"), task);
        }
        descriptorObject.put("tasks", tasks);
        return descriptorObject;
    }


    public Map<String, Object> addDescriptorForJson(String descriptor) {

        JsonNode descriptorJson = transToJSON(descriptor);

        String identity = descriptorJson.path("meta").path("identity").asText();
        String businessId = descriptorJson.path("meta").path("workspace").asText();
        String featureName = descriptorJson.path("meta").path("dagName").asText();
        String alias = descriptorJson.path("meta").path("alias").asText();

        JsonNode descriptorJsonNode = formatToFlowDescriptor(descriptorJson);
        String descriptorFormat = JsonToYaml(descriptorJsonNode);

        String md5 = DigestUtils.md5Hex(descriptorFormat);
        String descriptorId = businessId + ":" + featureName + ":" + "md5_" + md5;
        redisClient.set(descriptorId, descriptor);

        return addDescriptor(identity, businessId, featureName, alias, descriptorFormat);
    }

    private JsonNode formatToFlowDescriptor(JsonNode descriptorNode) {
        try {
            // output
            ObjectNode descriptorFormatNode = new ObjectMapper().createObjectNode();

            // handle nodes
            JsonNode nodes = descriptorNode.get("nodes");
            Map<String, ObjectNode> nodeMap = Maps.newHashMap();
            for (JsonNode node : nodes) {
                nodeMap.put(node.get("id").asText(), (ObjectNode) node);
            }

            // handle edges
            JsonNode edges = descriptorNode.get("edges");
            for (JsonNode edge : edges) {
                ObjectNode sourceNode = nodeMap.get(edge.get("source").asText());
                ObjectNode targetNode = nodeMap.get(edge.get("target").asText());
                if (Objects.isNull(sourceNode.get("next"))) {
                    sourceNode.put("next", targetNode.get("label").asText());
                } else {
                    sourceNode.put("next", sourceNode.get("next").asText() + "," + targetNode.get("label").asText());
                }
            }

            //tasks
            ArrayNode tasks = JsonNodeFactory.instance.arrayNode();
            for (JsonNode node : nodes) {
                //task
                ObjectNode task = new ObjectMapper().createObjectNode();
                task.set("category", node.path("nodeDetails").path("category"));
                task.set("name", node.get("label"));
                task.set("resourceName", node.path("nodeDetails").path("resource_name"));
                task.set("pattern", node.path("nodeDetails").path("pattern"));
                task.set("inputMappings", node.path("nodeDetails").path("input_mappings"));
                task.set("outputMappings", node.path("nodeDetails").path("output_mappings"));
                task.set("resourceProtocol", node.path("nodeDetails").path("resource_protocol"));
                task.set("parameters", node.path("nodeDetails").path("parameters"));
                task.set("next", node.get("next"));
                tasks.add(task);
            }

            // handle output
            descriptorFormatNode.set("version", descriptorNode.path("meta").path("version"));
            descriptorFormatNode.set("workspace", descriptorNode.path("meta").path("workspace"));
            descriptorFormatNode.set("dagName", descriptorNode.path("meta").path("dagName"));
            descriptorFormatNode.put("type", "flow");
            descriptorFormatNode.set("defaultContext", descriptorNode.path("meta").path("defaultContext"));
            descriptorFormatNode.set("commonMapping", descriptorNode.path("meta").path("commonMapping"));
            descriptorFormatNode.set("callback", descriptorNode.path("meta").path("callback"));
            descriptorFormatNode.set("tasks", tasks);

            return descriptorFormatNode;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String JsonToYaml(JsonNode jsonData) {
        try {
            // 创建 ObjectMapper 和 YAMLFactory
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            // 将 Java 对象转换为 YAML 字符串
            String yamlData = yamlMapper.writeValueAsString(jsonData);
            // 输出 YAML 数据
            return yamlData;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
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

    private JsonNode transToJSON(String descriptor) {
        if (StringUtils.isEmpty(descriptor)) {
            throw new RuntimeException();
        }
        try {
            return new ObjectMapper().readTree(descriptor);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public List<Map<String, Object>> getDagOpGroups() {
        List<Map<String, Object>> groups = new ArrayList<>();
        Node function = Node.builder()
                .id(1)
                .name("")
                .category(NodeType.FUNCTION.getType())
                .status("")
                .resourceName("")
                .pattern("task_sync")
                .inputMappings("")
                .outputMappings("")
                .tolerance("")
                .build();

        Node rillFlow = Node.builder()
                .id(2)
                .name("")
                .category(NodeType.RILL_FLOW.getType())
                .status("")
                .resourceName("rillflow://default:common:test:prod:test")
                .pattern("task_scheduler")
                .inputMappings("")
                .outputMappings("")
                .tolerance("")
                .build();
        JSONObject group3 = new JSONObject();
        group3.put("groupId", 1);
        group3.put("groupName", "流程节点");
        group3.put("operatorList", Arrays.asList(function, rillFlow));
        groups.add(group3);

        Node forNode = Node.builder()
                .id(4)
                .name("")
                .category(NodeType.FOREACH.getType())
                .status("")
                .resourceName("")
                .pattern("task_scheduler")
                .inputMappings("")
                .outputMappings("")
                .tolerance("")
                .build();

        Node returnNode = Node.builder()
                .id(5)
                .name("")
                .category(NodeType.RETURN.getType())
                .pattern("task_scheduler")
                .conditions("")
                .build();

        JSONObject group4 = new JSONObject();
        group4.put("groupId", 2);
        group4.put("groupName", "逻辑节点");
        group4.put("operatorList", Arrays.asList(forNode, returnNode));
        groups.add(group4);

        //plugins
        List<JSONObject> protocolPlugins = protocolPluginService.getProtocolPlugins();
        List<Node> pluginNodeList = Lists.newArrayList();
        for (int i = 0; i < protocolPlugins.size(); i++) {
            Node pluginNode = Node.builder()
                    .id(3 + i)
                    .name(protocolPlugins.get(i).getString("name"))
                    .icon(protocolPlugins.get(i).getString("icon"))
                    .schema(protocolPlugins.get(i).getString("schema"))
                    .category(NodeType.HTTP_HTTPS.getType())
                    .pattern("task_scheduler")
                    .conditions("")
                    .build();
            pluginNodeList.add(pluginNode);
        }
        JSONObject pluginsGroup = new JSONObject();
        pluginsGroup.put("groupId", 3);
        pluginsGroup.put("groupName", "插件节点");
        pluginsGroup.put("operatorList", pluginNodeList);
        groups.add(pluginsGroup);

        return groups;
    }
}
