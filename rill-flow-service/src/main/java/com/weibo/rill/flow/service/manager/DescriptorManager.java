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

package com.weibo.rill.flow.service.manager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.Expression;
import com.weibo.rill.flow.common.constant.ReservedConstant;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <pre>
 * DAG描述符规则
 * 1. DAG描述符id
 *    举例:
 *      businessId:feature:alias 如: testBusinessId:testFeatureName:release
 *      businessId:feature 如: testBusinessId:testFeatureName 按testFeatureName feature灰度策略选择alias 无灰度策略则alias用release
 *      businessId:feature:md5_描述符md5 如: testBusinessId:testFeatureName:md5_4297f44b13955235245b2497399d7a93
 *    1.1 businessId
 *        含义: 表示业务类型
 *        redisKey: business_id
 *        type: set
 *        member: 业务名称 如: testBusinessId
 *    1.2 feature
 *        含义: 表示业务的一种服务特征
 *        redisKey: feature_ + businessId 如: feature_testBusinessId
 *        type: set
 *        member: 服务名称 如: testFeatureName
 *    1.3 alias
 *        含义: 服务的描述符别名
 *        redisKey: alias_ + businessId + _ + feature 如: alias_testBusinessId_testFeatureName
 *        type: set
 *        member: 别名id 如: release/preview
 *    1.4 version
 *        含义: 别名下的各个版本
 *        redisKey: version_ + businessId + _ + feature + _ + alias 如: version_testBusinessId_testFeatureName_release
 *        type: zset
 *        member: 描述符md5
 *        score: 时间毫秒数
 *    1.5 gray
 *        含义: 服务灰度策略
 *        redisKey: gray_ + businessId + _ + feature 如: gray_testBusinessId_testFeatureName
 *        type: hash
 *        field: 别名
 *        value: 别名灰度策略
 *
 * 2. DAG描述符
 *    redisKey: descriptor_ + businessId + _ + feature + _ + 描述符md5 如: descriptor_testBusinessId_testFeatureName_md5
 *    type: string
 *    value: DAG描述符
 *
 * 3. 同一个业务的feature/alias/version/gray/DAG描述符 存储在一个端口上
 * </pre>
 * <p>
 * Created by xilong on 2021/8/18.
 */
@Slf4j
@Service
public class DescriptorManager {
    private final Pattern namePattern = Pattern.compile("^[a-zA-Z0-9]+$");

    private static final String BUSINESS_ID = "business_id";
    private static final String FEATURE_KEY_RULE = "feature_%s";
    private static final String ALIAS_KEY_RULE = "alias_%s_%s";
    private static final String VERSION_KEY_RULE = "version_%s_%s_%s";
    private static final String GRAY_KEY_RULE = "gray_%s_%s";
    private static final String AB_CONFIG_KEY_RULE = "abConfigKey_%s";
    private static final String FUNCTION_AB_KEY_RULE = "functionAB_%s_%s";
    private static final String DESCRIPTOR_KEY_RULE = "descriptor_%s_%s_%s";
    private static final String MD5_PREFIX = "md5_";
    private static final String RELEASE = "release";
    private static final String DEFAULT = "default";

    private static final String VERSION_ADD = """
            local maxVersionCount = ARGV[1];
            local versionKey = KEYS[1];

            local versionToDel = redis.call("zrange", versionKey, 0, -maxVersionCount);
            for i = 1, #versionToDel, 1 do
                local md5 = versionToDel[i];
                redis.call("zrem", versionKey, md5);
            end

            redis.call("zadd", versionKey, ARGV[2], ARGV[3]);
            redis.call("set", KEYS[2], ARGV[4]);

            return "OK";""";

    @Setter
    private int versionMaxCount = 300;

    @Autowired
    @Qualifier("descriptorRedisClient")
    private RedisClient redisClient;
    @Autowired
    private DAGStringParser dagParser;
    @Autowired
    private AviatorCache aviatorCache;
    @Autowired
    private SwitcherManager switcherManagerImpl;

    private final Cache<String, String> descriptorRedisKeyToYamlCache = CacheBuilder.newBuilder()
            .maximumSize(300)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .build();
    private final Cache<String, String> descriptorIdToRedisKeyCache = CacheBuilder.newBuilder()
            .maximumSize(300)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .build();

    public String getDagDescriptor(Long uid, Map<String, Object> input, String dagDescriptorId) {
        // 调用量比较小 useCache为false 实时取最新的yaml保证更新会立即生效
        return getDagDescriptorWithCache(uid, input, dagDescriptorId, false);
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
    public String getDagDescriptorWithCache(Long uid, Map<String, Object> input, String dagDescriptorId, boolean useCache) {
        try {
            // 校验dagDescriptorId
            String[] fields = StringUtils.isEmpty(dagDescriptorId) ? new String[0] : dagDescriptorId.trim().split(ReservedConstant.COLON);
            if (fields.length < 2 || nameInvalid(fields[0], fields[1])) {
                log.info("getDagDescriptor dagDescriptorId data format error, dagDescriptorId:{}", dagDescriptorId);
                throw new TaskException(BizError.ERROR_DATA_FORMAT.getCode(), "dagDescriptorId:" + dagDescriptorId + " format error");
            }

            // 获取dagDescriptorId对应的redisKey
            String businessId = fields[0];
            String featureName = fields[1];
            String thirdField = fields.length > 2 ? fields[2] : null;
            if (StringUtils.isEmpty(thirdField)) {
                thirdField = getDescriptorAliasByGrayRule(uid, input, businessId, featureName);
                log.info("getDagDescriptor result businessId:{} featureName:{} alias:{}", businessId, featureName, thirdField);
            }
            String descriptorRedisKey;
            if (thirdField.startsWith(MD5_PREFIX)) {
                descriptorRedisKey = buildDescriptorRedisKey(businessId, featureName, thirdField.replaceFirst(MD5_PREFIX, StringUtils.EMPTY));
            } else {
                String alias = thirdField;
                descriptorRedisKey = useCache ?
                        descriptorIdToRedisKeyCache.get(buildDescriptorId(businessId, featureName, alias),
                                () -> getDescriptorRedisKeyByAlias(businessId, featureName, alias)) :
                        getDescriptorRedisKeyByAlias(businessId, featureName, alias);
            }

            // 根据redisKey获取文件内容
            String descriptor = switcherManagerImpl.getSwitcherState("ENABLE_GET_DESCRIPTOR_FROM_CACHE") ?
                    descriptorRedisKeyToYamlCache.get(descriptorRedisKey, () -> getDescriptor(businessId, descriptorRedisKey)) :
                    getDescriptor(businessId, descriptorRedisKey);
            if (StringUtils.isEmpty(descriptor)) {
                throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("descriptor:%s value empty", dagDescriptorId));
            }
            return descriptor;
        } catch (TaskException taskException) {
            throw taskException;
        } catch (Exception e) {
            log.warn("getDagDescriptor fails, uid:{}, dagDescriptorId:{}", uid, dagDescriptorId, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("get descriptor:%s fails", dagDescriptorId));
        }
    }

    public BaseResource getTaskResource(Long uid, Map<String, Object> input, String resourceName) {
        try {
            URI uri = new URI(resourceName);

            String dagDescriptorId = uri.getAuthority();
            // 调用量比较大 useCache=tre 以减轻redis数据获取压力
            String dagDescriptor = getDagDescriptorWithCache(uid, input, dagDescriptorId, true);
            DAG dag = dagParser.parse(dagDescriptor);
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

    private String getDescriptor(String businessId, String descriptorRedisKey) {
        return redisClient.get(businessId, descriptorRedisKey);
    }

    private String getDescriptorAliasByGrayRule(Long uid, Map<String, Object> input, String businessId, String featureName) {
        Map<String, String> aliasToGrayRuleMap = getGray(businessId, featureName);
        log.info("getDescriptorAliasByGrayRule map empty:{}", MapUtils.isEmpty(aliasToGrayRuleMap));
        return getValueFromRuleMap(uid, input, aliasToGrayRuleMap, RELEASE);
    }

    private String getValueFromRuleMap(Long uid, Map<String, Object> input, Map<String, String> ruleMap, String defaultValue) {
        long aviatorUid = uid == null ? 0L : uid;
        Map<String, Object> aviatorInput = MapUtils.isEmpty(input) ? Collections.emptyMap() : input;
        return ruleMap.entrySet().stream()
                .filter(entry -> !containsEmpty(entry.getKey(), entry.getValue()))
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

    private String getDescriptorRedisKeyByAlias(String businessId, String featureName, String alias) {
        if (nameInvalid(businessId, featureName, alias)) {
            log.info("getDescriptorRedisKeyByAlias param invalid, businessId:{}, featureName:{}, alias:{}", businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        Set<String> redisRet = redisClient.zrange(businessId, buildVersionRedisKey(businessId, featureName, alias), -1, -1);
        if (CollectionUtils.isEmpty(redisRet)) {
            log.info("getDescriptorRedisKeyByAlias redisRet empty");
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("alias %s value empty", alias));
        }

        String md5 = redisRet.iterator().next();
        log.info("getDescriptorRedisKeyByAlias md5:{}", md5);
        return buildDescriptorRedisKey(businessId, featureName, md5);
    }

    public boolean createBusiness(String businessId) {
        if (nameInvalid(businessId)) {
            log.info("createBusiness params invalid, businessId:{}", businessId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.sadd(BUSINESS_ID, businessId);
        return true;
    }

    public boolean remBusiness(String businessId) {
        if (nameInvalid(businessId)) {
            log.info("remBusiness params invalid, businessId:{}", businessId);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(BUSINESS_ID, businessId);
        return true;
    }

    public Set<String> getBusiness() {
        return redisClient.smembers(BUSINESS_ID, BUSINESS_ID);
    }

    public boolean createFeature(String businessId, String featureName) {
        if (nameInvalid(businessId, featureName)) {
            log.info("createFeature params invalid, businessId:{}, serviceName:{}", businessId, featureName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        createBusiness(businessId);
        redisClient.sadd(businessId, buildFeatureRedisKey(businessId), Lists.newArrayList(featureName));
        return true;
    }

    public boolean remFeature(String businessId, String featureName) {
        if (nameInvalid(businessId, featureName)) {
            log.info("remFeature params invalid, businessId:{}, featureName:{}", businessId, featureName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(businessId, buildFeatureRedisKey(businessId), Lists.newArrayList(featureName));
        return true;
    }

    public Set<String> getFeature(String businessId) {
        return redisClient.smembers(businessId, buildFeatureRedisKey(businessId));
    }

    public boolean createAlias(String businessId, String featureName, String alias) {
        if (nameInvalid(businessId, featureName, alias)) {
            log.info("createAlias params invalid, businessId:{}, featureName:{}, alias:{}",
                    businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        createFeature(businessId, featureName);
        redisClient.sadd(businessId, buildAliasRedisKey(businessId, featureName), Lists.newArrayList(alias));
        return true;
    }

    public boolean remAlias(String businessId, String featureName, String alias) {
        if (nameInvalid(businessId, featureName, alias)) {
            log.info("remAlias params invalid, businessId:{}, featureName:{}, alias:{}", businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.srem(businessId, buildAliasRedisKey(businessId, featureName), Lists.newArrayList(alias));
        return true;
    }

    public Set<String> getAlias(String businessId, String featureName) {
        return redisClient.smembers(businessId, buildAliasRedisKey(businessId, featureName));
    }

    public boolean createGray(String businessId, String featureName, String alias, String grayRule) {
        if (StringUtils.isEmpty(grayRule) || nameInvalid(businessId, featureName, alias)) {
            log.info("createGray param invalid, businessId:{}, featureName:{}, aliasName:{}, grayRule:{}",
                    businessId, featureName, alias, grayRule);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        createAlias(businessId, featureName, alias);
        redisClient.hmset(businessId, buildGrayRedisKey(businessId, featureName), ImmutableMap.of(alias, grayRule));
        return true;
    }

    public boolean remGray(String businessId, String featureName, String alias) {
        if (nameInvalid(businessId, featureName, alias)) {
            log.info("remGray params invalid, businessId:{}, featureName:{}, alias:{}", businessId, featureName, alias);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.hdel(businessId, buildGrayRedisKey(businessId, featureName), Lists.newArrayList(alias));
        return true;
    }

    public Map<String, String> getGray(String businessId, String featureName) {
        return redisClient.hgetAll(businessId, buildGrayRedisKey(businessId, featureName));
    }

    public boolean createABConfigKey(String businessId, String configKey) {
        if (nameInvalid(businessId, configKey)) {
            log.info("createABConfigKey params invalid, businessId:{}, configKey:{}", businessId, configKey);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }
        redisClient.sadd(businessId, buildABConfigKeyRedisKey(businessId), Lists.newArrayList(configKey));
        return true;
    }

    public Set<String> getABConfigKey(String businessId) {
        return redisClient.smembers(businessId, buildABConfigKeyRedisKey(businessId));
    }

    public boolean createFunctionAB(String businessId, String configKey, String resourceName, String abRule) {
        if (nameInvalid(businessId, configKey) || containsEmpty(resourceName, abRule)) {
            log.info("createFunctionAB param invalid, businessId:{}, configKey:{}, resourceName:{}, abRule:{}", businessId, configKey, resourceName, abRule);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        createABConfigKey(businessId, configKey);

        if (!DEFAULT.equals(abRule) && StringUtils.isEmpty(getFunctionAB(businessId, configKey).getLeft())) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "default resource value should be configured");
        }
        String resourceNameStorage = DEFAULT.equals(abRule) ? DEFAULT + resourceName : resourceName;
        redisClient.hmset(businessId, buildFunctionABRedisKey(businessId, configKey), ImmutableMap.of(resourceNameStorage, abRule));
        return true;
    }

    public boolean remFunctionAB(String businessId, String configKey, String resourceName) {
        if (nameInvalid(businessId, configKey) || containsEmpty(resourceName)) {
            log.info("remFunctionAB params invalid, businessId:{}, configKey:{}, resourceName:{}", businessId, configKey, resourceName);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        redisClient.hdel(businessId, buildFunctionABRedisKey(businessId, configKey), Lists.newArrayList(resourceName));
        return true;
    }

    public Pair<String, Map<String, String>> getFunctionAB(String businessId, String configKey) {
        Map<String, String> redisRet = redisClient.hgetAll(businessId, buildFunctionABRedisKey(businessId, configKey));

        String defaultResourceName = null;
        Map<String, String> resourceNameToABRules = Maps.newHashMap();
        for (Map.Entry<String, String> resourceToRule : redisRet.entrySet()) {
            String resourceName = resourceToRule.getKey();
            String rule = resourceToRule.getValue();
            if (StringUtils.isNotEmpty(rule) && rule.equals(DEFAULT)) {
                defaultResourceName = resourceName.replaceFirst(DEFAULT, StringUtils.EMPTY);
            } else {
                resourceNameToABRules.put(resourceName, rule);
            }
        }

        return Pair.of(defaultResourceName, resourceNameToABRules);
    }

    public String calculateResourceName(Long uid, Map<String, Object> input, String executionId, String configKey) {
        String businessId = ExecutionIdUtil.getBusinessId(executionId);
        Pair<String, Map<String, String>> functionAB = getFunctionAB(businessId, configKey);
        String resourceName = getValueFromRuleMap(uid, input, functionAB.getRight(), functionAB.getLeft());
        log.info("calculateResourceName result resourceName:{} executionId:{} configKey:{}", resourceName, executionId, configKey);
        return resourceName;
    }

    public List<Map<String, ? extends Serializable>> getVersion(String businessId, String featureName, String alias) {
        Set<Pair<String, Double>> redisRet = redisClient.zrangeWithScores(businessId, buildVersionRedisKey(businessId, featureName, alias), 0, -1);
        return redisRet.stream()
                .map(memberToScore -> {
                    String md5 = memberToScore.getKey();
                    Long createTime = memberToScore.getValue().longValue();
                    return Pair.of(buildDescriptorId(businessId, featureName, MD5_PREFIX + md5), createTime);
                })
                .sorted((c1, c2) -> c2.getValue().compareTo(c1.getValue()))
                .map(idToCreateTime -> ImmutableMap.of("descriptor_id", idToCreateTime.getLeft(), "create_time", idToCreateTime.getRight()))
                .collect(Collectors.toList());
    }

    public String createDAGDescriptor(String businessId, String featureName, String alias, String descriptor) {
        if (StringUtils.isEmpty(descriptor) || nameInvalid(businessId, featureName, alias)) {
            log.info("createDAGDescriptor param invalid, businessId:{}, featureName:{}, alias:{}, descriptor:{}", businessId, featureName, alias, descriptor);
            throw new TaskException(BizError.ERROR_DATA_FORMAT);
        }

        DAG dag = dagParser.parse(descriptor);
        if (!businessId.equals(dag.getWorkspace()) || !featureName.equals(dag.getDagName())) {
            log.info("createDAGDescriptor businessId or featureName not match, businessId:{}, workspace:{}, featureName:{}, dagName:{}",
                    businessId, dag.getWorkspace(), featureName, dag.getDagName());
            throw new TaskException(BizError.ERROR_DATA_FORMAT, "name not match");
        }

        createAlias(businessId, featureName, alias);

        String md5 = DigestUtils.md5Hex(descriptor);

        List<String> keys = Lists.newArrayList();
        List<String> argv = Lists.newArrayList();
        keys.add(buildVersionRedisKey(businessId, featureName, alias));
        keys.add(buildDescriptorRedisKey(businessId, featureName, md5));
        argv.add(String.valueOf(versionMaxCount));
        argv.add(String.valueOf(System.currentTimeMillis()));
        argv.add(md5);
        argv.add(descriptor);
        redisClient.eval(VERSION_ADD, businessId, keys, argv);

        return buildDescriptorId(businessId, featureName, MD5_PREFIX + md5);
    }

    private boolean containsEmpty(String... member) {
        return member == null || Arrays.stream(member).anyMatch(StringUtils::isEmpty);
    }

    private boolean nameInvalid(String... names) {
        return containsEmpty(names) || Arrays.stream(names).anyMatch(name -> !namePattern.matcher(name).find());
    }

    private String buildFeatureRedisKey(String businessId) {
        return String.format(FEATURE_KEY_RULE, businessId);
    }

    private String buildAliasRedisKey(String namespace, String serviceName) {
        return String.format(ALIAS_KEY_RULE, namespace, serviceName);
    }

    private String buildGrayRedisKey(String namespace, String serviceName) {
        return String.format(GRAY_KEY_RULE, namespace, serviceName);
    }

    private String buildABConfigKeyRedisKey(String businessId) {
        return String.format(AB_CONFIG_KEY_RULE, businessId);
    }

    private String buildFunctionABRedisKey(String businessId, String configKey) {
        return String.format(FUNCTION_AB_KEY_RULE, businessId, configKey);
    }

    private String buildVersionRedisKey(String businessId, String featureName, String alias) {
        return String.format(VERSION_KEY_RULE, businessId, featureName, alias);
    }

    private String buildDescriptorRedisKey(String businessId, String featureName, String md5) {
        return String.format(DESCRIPTOR_KEY_RULE, businessId, featureName, md5);
    }

    private String buildDescriptorId(String businessId, String featureName, String thirdPart) {
        List<String> ids = Lists.newArrayList(businessId, featureName);
        Optional.ofNullable(thirdPart).ifPresent(ids::add);
        return StringUtils.join(ids, ReservedConstant.COLON);
    }
}
