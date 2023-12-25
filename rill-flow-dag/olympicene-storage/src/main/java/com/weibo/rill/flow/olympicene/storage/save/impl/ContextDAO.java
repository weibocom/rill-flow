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

package com.weibo.rill.flow.olympicene.storage.save.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.storage.constant.DAGRedisPrefix;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.exception.StorageException;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.script.RedisScriptManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * <pre>
 * 存储结构说明
 * 1. context为{@link Map Map}类型
 *   1.1 key
 *       类型: {@link String String}
 *   1.2 value
 *       类型: {@link String String} {@link List List} {@link Map Map} 及其他基本数据类型
 *            尽量避免使用自定义类, 循环引用/未加载该类等其他原因 可能会导致序列化失败
 *   1.3 约定
 *       key以 __ 开头, 代表value为子context内容, 子context类型及取值与context相同
 *       value不能为null 若value为子context, 则value不能为空map
 *
 * 2. 目标
 *    context及子context每个value均可独立更新
 *
 * 3. context举例
 *    {
 *      "url": "http://test.com/test",
 *      "segments": [
 *        "gopUrl1",
 *        "gopUrl2"
 *      ],
 *      "__B_0": {
 *        "gopPath": "gopPath1",
 *        "segmentUrl": "gopUrl1",
 *        "segments": [
 *          "gopUrl1",
 *          "gopUrl2"
 *        ]
 *      },
 *      "__B_1": {
 *        "segmentUrl": "gopUrl2",
 *        "segments": [
 *          "gopUrl1",
 *          "gopUrl2"
 *        ]
 *      }
 *    }
 *
 * 4. redis 存储内容说明
 *   4.1 context
 *       类型: hash
 *       key: context_ + executionId
 *       field                   |   value
 *       "url"                   |   "http://test.com/test"
 *       "segments"              |   ["gopUrl1","gopUrl2"]
 *       "@subContextName___B_0" |   "__B_0"
 *       "@subContextName___B_1" |   "__B_1"                对于子context
 *                                                          field: @subContextName_ + contextName
 *                                                          value: contextName
 *   4.2 子context映射
 *       类型: hash
 *       key: context_mapping_ + executionId
 *       field   | value
 *       "__B_0" | "sub_context_executionId___B_0"
 *       "__B_1" | "sub_context_executionId___B_1"   field: 子contextName value: 存子context内容的redis key
 *
 *   4.3 子context
 *       类型: hash
 *       key: sub_context_ + executionId + "_" + contextName
 *       field/value 同context
 * </pre>
 *
 * @see DAGInfoDAO
 */
@Slf4j
public class ContextDAO {
    private static final String ROOT_LEVEL_CONTEXT = "@rootContext";
    private static final String REDIS_SUB_CONTEXT_NAME_PREFIX = "@subContextName_";

    private final RedisClient redisClient;
    private final int finishStatusReserveTimeInSecond;
    private final int unfinishedStatusReserveTimeInSecond;

    public ContextDAO(RedisClient redisClient) {
        this.redisClient = redisClient;
        this.unfinishedStatusReserveTimeInSecond = 2 * 24 * 3600;
        this.finishStatusReserveTimeInSecond = 0;
    }

    public ContextDAO(RedisClient redisClient, int unfinishedStatusReserveTimeInSecond, int finishStatusReserveTimeInSecond) {
        this.redisClient = redisClient;
        this.unfinishedStatusReserveTimeInSecond = unfinishedStatusReserveTimeInSecond;
        this.finishStatusReserveTimeInSecond = finishStatusReserveTimeInSecond;
    }

    // ------------------------------------------------------
    // 若想动态修改属性值 如: 不同业务设置不同值
    // 需要继承该类 重写该方法 添加动态设置业务逻辑
    protected int getFinishStatusReserveTimeInSecond(String executionId) {
        log.debug("getFinishStatusReserveTimeInSecond executionId:{}, time:{}", executionId, finishStatusReserveTimeInSecond);
        return finishStatusReserveTimeInSecond;
    }

    protected int getUnfinishedStatusReserveTimeInSecond(String executionId) {
        log.debug("getUnfinishedStatusReserveTimeInSecond executionId:{}, time:{}", executionId, unfinishedStatusReserveTimeInSecond);
        return unfinishedStatusReserveTimeInSecond;
    }

    protected boolean enableContextLengthCheck(String executionId) {
        log.debug("enableContextLengthCheck skip executionId:{}", executionId);
        return false;
    }

    protected int contextMaxLength(String executionId) {
        log.debug("contextMaxLength length:10000 executionId:{}", executionId);
        return 10000;
    }
    // 目前只支持修改时间及context长度检查设置
    // ------------------------------------------------------

    public Map<String, Object> getContext(String executionId, boolean needSubContext) {
        try {
            log.info("getContext executionId:{} needSubContext:{}", executionId, needSubContext);
            List<List<List<byte[]>>> contextByte = getContextFromRedis(executionId, needSubContext);
            if (CollectionUtils.isEmpty(contextByte)) {
                return Maps.newHashMap();
            }

            List<byte[]> contents = contextByte.stream()
                    .map(array -> array.get(1))
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMap(Collection::stream)
                    .toList();

            if (!needSubContext) {
                checkContextLength(executionId, contents);
            }

            return buildContext(executionId, contextByte);
        } catch (Exception e) {
            log.warn("getContext fails, executionId:{}", executionId, e);
            throw e;
        }
    }

    private List<List<List<byte[]>>> getContextFromRedis(String executionId, boolean needSubContext) {
        List<String> keys = !needSubContext ?
                Lists.newArrayList(buildContextRedisKey(executionId)) :
                Lists.newArrayList(buildContextRedisKey(executionId), buildContextNameToContextRedisKey(executionId));
        return (List<List<List<byte[]>>>) redisClient.eval(
                RedisScriptManager.getRedisGet(), executionId, keys, Lists.newArrayList());
    }

    protected void checkContextLength(String executionId, List<byte[]> contents) {
        if (!enableContextLengthCheck(executionId) || CollectionUtils.isEmpty(contents)) {
            return;
        }

        int length = contents.stream().filter(Objects::nonNull).mapToInt(content -> content.length).sum();
        if (length > contextMaxLength(executionId)) {
            throw new StorageException(
                    StorageErrorCode.CONTEXT_LENGTH_LIMITATION.getCode(),
                    String.format("content length:%s exceed the limit", length));
        }
    }

    private Map<String, Object> buildContext(String executionId, List<List<List<byte[]>>> contextByte) {
        Map<String, Map<String, Object>> contextNameToContentMap = deserializeContext(contextByte);

        Map<String, Object> rootContext = contextNameToContentMap.get(buildContextRedisKey(executionId));
        if (MapUtils.isEmpty(rootContext)) {
            return Maps.newHashMap();
        }
        appendSubContext(1, rootContext, contextNameToContentMap);

        return rootContext;
    }

    private Map<String, Map<String, Object>> deserializeContext(List<List<List<byte[]>>> contextBytes) {
        Map<String, Map<String, Object>> contextNameToContext = Maps.newLinkedHashMap();
        contextBytes.forEach(context -> {
            List<byte[]> setting = context.get(0);
            List<byte[]> contextByte = context.get(1);
            contextNameToContext.put(DagStorageSerializer.getString(setting.get(1)), DagStorageSerializer.deserializeHash(contextByte));
        });
        return contextNameToContext;
    }

    private void appendSubContext(int depth, Map<String, Object> context, Map<String, Map<String, Object>> contextNameToContentMap) {
        if (depth >= SystemConfig.getTaskMaxDepth()) {
            return;
        }

        Set<String> subContextNames = removeSubContextPlaceholder(context);
        if (CollectionUtils.isEmpty(subContextNames)) {
            return;
        }

        Map<String, Map<String, Object>> subContext = subContextNames.stream()
                .filter(contextNameToContentMap::containsKey)
                .collect(Collectors.toMap(subContextName -> subContextName, contextNameToContentMap::get));
        if (MapUtils.isNotEmpty(subContext)) {
            context.putAll(subContext);
            subContext.values().forEach(sub -> appendSubContext(depth + 1, sub, contextNameToContentMap));
        }
    }

    private Set<String> removeSubContextPlaceholder(Map<String, Object> context) {
        return Lists.newArrayList(context.keySet()).stream()
                .filter(key -> key.startsWith(REDIS_SUB_CONTEXT_NAME_PREFIX))
                .map(context::remove)
                .filter(Objects::nonNull)
                .map(value -> (String) value)
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getContext(String executionId, Collection<String> fields) {
        try {
            log.info("getContext executionId:{} fields:{}", executionId, fields);
            // fields按类型存入 rootContextFields subContextNames
            List<String> rootContextFields = Lists.newArrayList();
            List<String> subContextNames = Lists.newArrayList();
            distinguishField(fields, rootContextFields, subContextNames);
            if (CollectionUtils.isEmpty(rootContextFields) && CollectionUtils.isEmpty(subContextNames)) {
                return Maps.newHashMap();
            }

            // 根据rootContextFields subContextNames构造eval需要的keys和argv参数 并调redis
            List<String> keys = Lists.newArrayList();
            List<String> argv = Lists.newArrayList();
            buildEvalParam(executionId, rootContextFields, subContextNames, keys, argv);

            // 根据keys和argv取context内容
            List<List<byte[]>> contextBytes = (List<List<byte[]>>) redisClient.eval(
                    RedisScriptManager.getRedisGetByFieldAndKey(), executionId, keys, argv);

            // 获取keys对应的返回值
            if (CollectionUtils.isEmpty(contextBytes) || contextBytes.size() != keys.size()) {
                throw new StorageException(StorageErrorCode.CONTEXT_GET_FAIL.getCode(), "context size not match key size");
            }

            List<byte[]> contents = contextBytes.stream()
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMap(Collection::stream)
                    .toList();

            if (CollectionUtils.isEmpty(subContextNames)) {
                checkContextLength(executionId, contents);
            }

            Map<String, List<byte[]>> redisKeyToContent = Maps.newHashMap();
            for (int i = 0; i < keys.size(); i++) {
                redisKeyToContent.put(keys.get(i), contextBytes.get(i));
            }

            // 根据rootContextFields subContextNames构造context值
            return buildContext(executionId, rootContextFields, subContextNames, redisKeyToContent);
        } catch (Exception e) {
            log.warn("getContext fails, executionId:{}", executionId, e);
            throw e;
        }
    }

    private Map<String, Object> buildContext(String executionId,
                                             List<String> rootContextFields, List<String> subContextNames,
                                             Map<String, List<byte[]>> redisKeyToContent) {
        Map<String, Object> context = Maps.newHashMap();

        subContextNames.forEach(subContextName -> {
            List<byte[]> subContextRedis = redisKeyToContent.get(buildSubContextRedisKey(executionId, subContextName));
            if (CollectionUtils.isEmpty(subContextRedis)) {
                log.info("buildContext can not get subContext, subContextName:{}", subContextName);
                return;
            }
            Map<String, Object> subContext = DagStorageSerializer.deserializeHash(subContextRedis);
            removeSubContextPlaceholder(subContext);
            context.put(subContextName, subContext);
        });

        if (CollectionUtils.isEmpty(rootContextFields)) {
            return context;
        }

        List<byte[]> rootContextRedis = redisKeyToContent.get(buildContextRedisKey(executionId));
        if (CollectionUtils.isEmpty(rootContextRedis) || rootContextRedis.size() != rootContextFields.size() * 2) {
            throw new StorageException(StorageErrorCode.CONTEXT_GET_FAIL.getCode(), "root context size not match");
        }

        List<byte[]> rootContext = Lists.newArrayList();
        for (int i = 0; i < rootContextFields.size(); i++) {
            String field = rootContextFields.get(i);
            byte[] value = rootContextRedis.get(i * 2);
            byte[] valueType = rootContextRedis.get(i * 2 + 1);
            if (value == null || valueType == null) {
                log.info("buildContext can not get value, field:{}", field);
                continue;
            }
            rootContext.add(DagStorageSerializer.getBytes(field));
            rootContext.add(value);
            rootContext.add(DagStorageSerializer.getBytes(DagStorageSerializer.buildTypeKeyPrefix(field)));
            rootContext.add(valueType);
        }
        Optional.of(rootContext)
                .filter(CollectionUtils::isNotEmpty)
                .ifPresent(it -> context.putAll(DagStorageSerializer.deserializeHash(it)));

        return context;
    }

    private void buildEvalParam(String executionId, List<String> rootContextFields, List<String> subContextNames, List<String> keys, List<String> argv) {
        if (CollectionUtils.isNotEmpty(rootContextFields)) {
            keys.add(buildContextRedisKey(executionId));
            rootContextFields.forEach(rootContextField -> {
                argv.add(rootContextField); // 当前field对应的value值
                argv.add(DagStorageSerializer.buildTypeKeyPrefix(rootContextField)); // 当前field对应value的类型
            });
        }
        subContextNames.stream()
                .map(subContextName -> buildSubContextRedisKey(executionId, subContextName))
                .forEach(keys::add);
    }

    private void distinguishField(Collection<String> fields, List<String> rootContextFields, List<String> subContextNames) {
        if (CollectionUtils.isEmpty(fields)) {
            return;
        }

        fields.stream().filter(StringUtils::isNotEmpty).forEach(field -> {
            if (field.startsWith(ReservedConstant.SUB_CONTEXT_PREFIX)) {
                subContextNames.add(field);
            } else {
                rootContextFields.add(field);
            }
        });
    }

    public void deleteContext(String executionId) {
        deleteContext(executionId, getFinishStatusReserveTimeInSecond(executionId));
    }

    public void deleteContext(String executionId, int expireTimeInSecond) {
        try {
            if (expireTimeInSecond < 0) {
                return;
            }
            log.info("deleteContext executionId:{} expireTime:{}", executionId, expireTimeInSecond);
            redisClient.eval(RedisScriptManager.getRedisExpire(),
                    executionId,
                    Lists.newArrayList(buildContextRedisKey(executionId), buildContextNameToContextRedisKey(executionId)),
                    Lists.newArrayList(String.valueOf(expireTimeInSecond)));
        } catch (Exception e) {
            log.warn("deleteContext fails, executionId:{}, expireTimeInSecond:{}", executionId, expireTimeInSecond, e);
            throw e;
        }
    }

    public void updateContext(String executionId, Map<String, Object> context) {
        if (MapUtils.isEmpty(context)) {
            return;
        }

        try {
            log.info("updateContext executionId:{}", executionId);

            List<String> keys = Lists.newArrayList();
            List<String> argv = Lists.newArrayList();
            serializeContext(executionId, context, keys, argv);

            redisClient.eval(RedisScriptManager.getRedisSetWithExpire(), executionId, keys, argv);
        } catch (Exception e) {
            log.warn("updateContext fails, executionId:{}", executionId, e);
            throw e;
        }
    }

    private void serializeContext(String executionId, Map<String, Object> context, List<String> keys, List<String> argv) {
        argv.add(String.valueOf(getUnfinishedStatusReserveTimeInSecond(executionId)));

        Map<String, Map<String, Object>> contextNameToContentMap = getContextNameToContentMap(1, ROOT_LEVEL_CONTEXT, context);

        Map<String, Object> rootContext = contextNameToContentMap.get(ROOT_LEVEL_CONTEXT);
        if (MapUtils.isNotEmpty(rootContext)) {
            keys.add(buildContextRedisKey(executionId));
            argv.add(ReservedConstant.PLACEHOLDER);
            argv.addAll(DagStorageSerializer.serializeHashToList(rootContext));
        }

        Map<String, String> subContextNameToRedisKey = Maps.newHashMap();
        contextNameToContentMap.forEach((contextName, contextContent) -> {
            if (MapUtils.isEmpty(contextContent) || ROOT_LEVEL_CONTEXT.equals(contextName)) {
                return;
            }

            String subContextRedisKey = buildSubContextRedisKey(executionId, contextName);
            keys.add(subContextRedisKey);
            argv.add(ReservedConstant.PLACEHOLDER);
            argv.addAll(DagStorageSerializer.serializeHashToList(contextContent));
            subContextNameToRedisKey.put(contextName, subContextRedisKey);
        });

        if (MapUtils.isNotEmpty(subContextNameToRedisKey)) {
            keys.add(buildContextNameToContextRedisKey(executionId));
            argv.add(ReservedConstant.PLACEHOLDER);
            subContextNameToRedisKey.forEach((filed, value) -> {
                argv.add(filed);
                argv.add(value);
            });
        }
    }

    private Map<String, Map<String, Object>> getContextNameToContentMap(int depth, String contextName, Map<String, Object> context) {
        Map<String, Map<String, Object>> contextNameToContentMap = Maps.newHashMap();
        Map<String, Object> currentContext = contextNameToContentMap.computeIfAbsent(contextName, k -> Maps.newHashMap());

        if (depth >= SystemConfig.getTaskMaxDepth()) {
            currentContext.putAll(context);
            return contextNameToContentMap;
        }

        context.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            if (!key.startsWith(ReservedConstant.SUB_CONTEXT_PREFIX)) {
                currentContext.put(key, value);
                return;
            }

            if (!(value instanceof Map)) {
                throw new StorageException(StorageErrorCode.CLASS_TYPE_NONSUPPORT.getCode(), "value type is not map");
            }
            currentContext.put(REDIS_SUB_CONTEXT_NAME_PREFIX + key, key);
            contextNameToContentMap.putAll(getContextNameToContentMap(depth + 1, key, (Map<String, Object>) value));
        });
        return contextNameToContentMap;
    }

    private String buildContextRedisKey(String executionId) {
        return DAGRedisPrefix.PREFIX_CONTEXT.getValue() + executionId;
    }

    private String buildContextNameToContextRedisKey(String executionId) {
        return DAGRedisPrefix.PREFIX_CONTEXT_MAPPING.getValue() + executionId;
    }

    private String buildSubContextRedisKey(String executionId, String key) {
        return DAGRedisPrefix.PREFIX_SUB_CONTEXT.getValue() + executionId + "_" + key;
    }
}
