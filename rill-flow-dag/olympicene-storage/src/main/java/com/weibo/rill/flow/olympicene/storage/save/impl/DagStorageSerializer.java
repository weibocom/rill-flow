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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.exception.SerializationException;
import com.weibo.rill.flow.olympicene.ddl.serialize.ObjectMapperFactory;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public class DagStorageSerializer {
    private static final String TYPE_PLACEHOLDER = "@class";
    public static final ObjectMapper MAPPER = ObjectMapperFactory.getJSONMapper();

    public static byte[] serialize(Object object) {
        try {
            return MAPPER.writeValueAsBytes(object);
        } catch (IOException e) {
            throw new SerializationException(StorageErrorCode.SERIALIZATION_FAIL.getCode(), e);
        }
    }

    public static String serializeToString(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (IOException e) {
            throw new SerializationException(StorageErrorCode.SERIALIZATION_FAIL.getCode(), e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new SerializationException(StorageErrorCode.SERIALIZATION_FAIL.getCode(), e);
        }
    }

    public static Map<String, Object> deserializeHash(List<byte[]> content) {
        if (CollectionUtils.isEmpty(content)) {
            return Maps.newHashMap();
        }

        Map<String, byte[]> stringByteContent = Lists.partition(content, 2).stream()
                .filter(array -> array.size() == 2)
                .collect(Collectors.toMap(array -> getString(array.get(0)), array -> array.get(1)));

        Map<String, Object> map = Maps.newHashMap();
        stringByteContent.forEach((field, value) -> {
            try {
                if (StringUtils.isEmpty(field) || field.startsWith(TYPE_PLACEHOLDER)) {
                    return;
                }

                String className = getString(stringByteContent.get(buildTypeKeyPrefix(field)));
                Class<?> klass = className != null ? Class.forName(className) : Object.class;
                map.put(field, MAPPER.readValue(value, klass));
            } catch (Exception e) {
                log.warn("deserializeHash fails, field:{}", field, e);
                throw new SerializationException(StorageErrorCode.SERIALIZATION_FAIL.getCode(), e);
            }
        });
        return map;
    }

    public static Map<String, String> serializeHash(Map<String, ?> content) {
        if (MapUtils.isEmpty(content)) {
            return Maps.newHashMap();
        }

        Map<String, String> serializedContent = Maps.newHashMap();
        content.forEach((field, value) -> {
            try {
                serializedContent.put(field, MAPPER.writeValueAsString(value));
                serializedContent.put(buildTypeKeyPrefix(field), value.getClass().getName());
            } catch (Exception e) {
                log.warn("serializeHash fails, field:{}, value:{}", field, value, e);
                throw new SerializationException(StorageErrorCode.SERIALIZATION_FAIL.getCode(), e);
            }
        });
        return serializedContent;
    }

    public static List<String> serializeHashToList(Map<String, ?> content) {
        List<String> ret = Lists.newArrayList();
        serializeHash(content).forEach((key, value) -> {
            ret.add(key);
            ret.add(value);
        });
        return ret;
    }

    public static String getString(byte[] v) {
        return v == null ? null : new String(v, StandardCharsets.UTF_8);
    }

    public static byte[] getBytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    public static String buildTypeKeyPrefix(String key) {
        return TYPE_PLACEHOLDER + "_" + key;
    }
}
