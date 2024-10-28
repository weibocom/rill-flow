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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.googlecode.aviator.Expression;
import com.weibo.rill.flow.common.constant.ReservedConstant;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO;
import com.weibo.rill.flow.service.converter.DAGDescriptorConverter;
import com.weibo.rill.flow.service.manager.AviatorCache;
import com.weibo.rill.flow.service.storage.dao.*;
import com.weibo.rill.flow.service.util.DAGStorageKeysUtil;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.weibo.rill.flow.service.util.DAGStorageKeysUtil.MD5_PREFIX;

@Service
@Slf4j
public class DAGDescriptorService {
    @Autowired
    private DAGAliasDAO dagAliasDAO;
    @Autowired
    private DAGFeatureDAO dagFeatureDAO;
    @Autowired
    private DAGDescriptorDAO dagDescriptorDAO;
    @Autowired
    private DAGDescriptorConverter dagDescriptorConverter;
    @Autowired
    private DAGGrayDAO dagGrayDAO;
    @Autowired
    private DAGBusinessDAO dagBusinessDAO;
    @Autowired
    private AviatorCache aviatorCache;
    @Autowired
    private DAGABTestDAO dagABTestDAO;

    private final Cache<String, String> descriptorIdToRedisKeyCache = CacheBuilder.newBuilder()
            .maximumSize(300)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .build();

    public DAG getDAG(Long uid, Map<String, Object> input, String dagDescriptorId) {
        // 调用量比较小 useCache为false 实时取最新的yaml保证更新会立即生效
        DescriptorPO descriptorPO = getDescriptorPO(uid, input, dagDescriptorId, false);
        return dagDescriptorConverter.convertDescriptorPOToDAG(descriptorPO);
    }

    public String saveDescriptorVO(String businessId, String featureName, String alias, DescriptorVO descriptorVO) {
        if (descriptorVO == null || DAGStorageKeysUtil.nameInvalid(businessId, featureName, alias)) {
            log.info("saveDescriptorVO param invalid, businessId:{}, featureName:{}, alias:{}, descriptor:{}", businessId, featureName, alias, descriptorVO);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        DAG dag = dagDescriptorConverter.convertDescriptorVOToDAG(descriptorVO);
        if (!businessId.equals(dag.getWorkspace()) || !featureName.equals(dag.getDagName())) {
            log.info("saveDescriptorVO businessId or featureName not match, businessId:{}, workspace:{}, featureName:{}, dagName:{}",
                    businessId, dag.getWorkspace(), featureName, dag.getDagName());
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "name not match");
        }

        dagBusinessDAO.createBusiness(businessId);
        dagFeatureDAO.createFeature(businessId, featureName);
        dagAliasDAO.createAlias(businessId, featureName, alias);

        DescriptorPO descriptorPO = dagDescriptorConverter.convertDAGToDescriptorPO(dag);
        return dagDescriptorDAO.persistDescriptorPO(businessId, featureName, alias, descriptorPO);
    }

    public BaseResource getTaskResource(Long uid, Map<String, Object> input, String resourceName) {
        try {
            URI uri = new URI(resourceName);

            String dagDescriptorId = uri.getAuthority();
            // 调用量比较大 useCache=tre 以减轻redis数据获取压力
            DescriptorPO dagDescriptorPO = getDescriptorPO(uid, input, dagDescriptorId, true);
            DAG dag = dagDescriptorConverter.convertDescriptorPOToDAG(dagDescriptorPO);
            if (CollectionUtils.isEmpty(dag.getResources())) {
                throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "dag resources empty");
            }

            Map<String, BaseResource> resourceMap = dag.getResources().stream()
                    .collect(Collectors.toMap(BaseResource::getName, it -> it));
            Map<String, String> queryParams = new URIBuilder(uri).getQueryParams().stream()
                    .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue, (v1, v2) -> v1));
            BaseResource baseResource = resourceMap.get(queryParams.get("name"));
            if (baseResource == null) {
                throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "dag resource null");
            }
            return baseResource;
        } catch (TaskException e) {
            throw e;
        } catch (Exception e) {
            log.warn("getTaskResource form dag config fails, resourceName:{}", resourceName, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "getTaskResource fails: " + e.getMessage(), e.getCause());
        }
    }

    public String calculateResourceName(Long uid, Map<String, Object> input, String executionId, String configKey) {
        String businessId = ExecutionIdUtil.getBusinessId(executionId);
        Pair<String, Map<String, String>> functionAB = dagABTestDAO.getFunctionAB(businessId, configKey);
        String resourceName = getValueFromRuleMap(uid, input, functionAB.getRight(), functionAB.getLeft());
        log.info("calculateResourceName result resourceName:{} executionId:{} configKey:{}", resourceName, executionId, configKey);
        return resourceName;
    }

    /**
     * @param useCache 是否使用缓存:descriptorIdToRedisKeyCache
     * <pre>
     * 先根据descriptorId获取其对应的redisKey，再根据redisKey取对应版本的yaml文件具体内容
     *
     * 该逻辑对应两个缓存
     * 1. descriptorIdToRedisKeyCache
     *    descriptorId最近更新版本yaml文件在redis存储的key
     *    如：testBusinessId:testFeatureName:release -> testBusinessId:testFeatureName:md5_4297f44b13955235245b2497399d7a93
     * 2. descriptorRedisKeyToYamlCache
     *    redisKey与yaml文件一一对应 所以该缓存默认启用
     *    如: testBusinessId:testFeatureName:md5_4297f44b13955235245b2497399d7a93 -> yaml
     *
     * </pre>
     */
    private DescriptorPO getDescriptorPO(Long uid, Map<String, Object> input, String dagDescriptorId, boolean useCache) {
        try {
            // 校验dagDescriptorId
            String[] fields = StringUtils.isEmpty(dagDescriptorId) ? new String[0] : dagDescriptorId.trim().split(ReservedConstant.COLON);
            if (fields.length < 2 || DAGStorageKeysUtil.nameInvalid(fields[0], fields[1])) {
                log.info("getDescriptorPO dagDescriptorId data format error, dagDescriptorId:{}", dagDescriptorId);
                throw new TaskException(BizError.ERROR_DATA_FORMAT.getCode(), "dagDescriptorId:" + dagDescriptorId + " format error");
            }

            // 获取dagDescriptorId对应的redisKey
            String businessId = fields[0];
            String featureName = fields[1];
            String thirdField = fields.length > 2 ? fields[2] : null;
            if (StringUtils.isEmpty(thirdField)) {
                thirdField = getDescriptorAliasByGrayRule(uid, input, businessId, featureName);
                log.info("getDescriptorPO result businessId:{} featureName:{} alias:{}", businessId, featureName, thirdField);
            }
            String descriptorRedisKey;
            if (thirdField.startsWith(DAGStorageKeysUtil.MD5_PREFIX)) {
                descriptorRedisKey = DAGStorageKeysUtil.buildDescriptorRedisKey(businessId, featureName, thirdField.replaceFirst(MD5_PREFIX, StringUtils.EMPTY));
            } else {
                String alias = thirdField;
                descriptorRedisKey = useCache ?
                        descriptorIdToRedisKeyCache.get(DAGStorageKeysUtil.buildDescriptorId(businessId, featureName, alias),
                                () -> dagAliasDAO.getDescriptorRedisKeyByAlias(businessId, featureName, alias)) :
                        dagAliasDAO.getDescriptorRedisKeyByAlias(businessId, featureName, alias);
            }

            return dagDescriptorDAO.getDescriptorPO(dagDescriptorId, descriptorRedisKey, businessId);
        } catch (TaskException taskException) {
            throw taskException;
        } catch (Exception e) {
            log.warn("getDescriptorPO fails, uid:{}, dagDescriptorId:{}", uid, dagDescriptorId, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("get descriptor:%s fails", dagDescriptorId));
        }
    }

    private String getValueFromRuleMap(Long uid, Map<String, Object> input, Map<String, String> ruleMap, String defaultValue) {
        long aviatorUid = uid == null ? 0L : uid;
        Map<String, Object> aviatorInput = MapUtils.isEmpty(input) ? Collections.emptyMap() : input;
        return ruleMap.entrySet().stream()
                .filter(entry -> !DAGStorageKeysUtil.containsEmpty(entry.getKey(), entry.getValue()))
                .sorted(Map.Entry.comparingByKey())
                .filter(entry -> {
                    try {
                        Map<String, Object> env = Maps.newHashMap();
                        env.put("uid", aviatorUid);
                        env.put("input", aviatorInput);
                        Expression expression = aviatorCache.getAviatorExpression(entry.getValue());
                        return (boolean) expression.execute(env);
                    } catch (Exception e) {
                        log.warn("getValueFromRuleMap execute fail, key:{}, value:{}", entry.getKey(), entry.getValue(), e);
                        return false;
                    }
                })
                .map(Map.Entry::getKey).findFirst().orElse(defaultValue);
    }

    private String getDescriptorAliasByGrayRule(Long uid, Map<String, Object> input, String businessId, String featureName) {
        Map<String, String> aliasToGrayRuleMap = dagGrayDAO.getGray(businessId, featureName);
        log.info("getDescriptorAliasByGrayRule map empty:{}", MapUtils.isEmpty(aliasToGrayRuleMap));
        return getValueFromRuleMap(uid, input, aliasToGrayRuleMap, DAGStorageKeysUtil.RELEASE);
    }
}
