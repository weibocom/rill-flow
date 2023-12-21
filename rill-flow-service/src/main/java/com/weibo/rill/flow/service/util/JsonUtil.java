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

package com.weibo.rill.flow.service.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.google.common.base.Strings;
import com.weibo.rill.flow.olympicene.ddl.serialize.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * @author zhangxin26
 */
public class JsonUtil {

    private JsonUtil() {
        // empty
    }

    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    private static ObjectMapper mapper;
    private static ObjectMapper mapperWithDefaultTyping;

    static {
        mapper = ObjectMapperFactory.getJSONMapper();
        mapperWithDefaultTyping = mapper.copy();
        mapperWithDefaultTyping.activateDefaultTyping(
            LaissezFaireSubTypeValidator.instance, 
            ObjectMapper.DefaultTyping.NON_FINAL
        );
    }

    public static <T> T parseObject(final String json, final Class<T> type) throws IOException {
        return mapper.readValue(json, type);
    }

    public static <T> T parseObjectWithType(final String json, final Class<T> type) throws IOException {
        return mapperWithDefaultTyping.readValue(json, type);
    }

    public static <T> T parseObject(String json, final TypeReference<T> type) {
        if (Strings.isNullOrEmpty(json)) {
            return null;
        }
        try {
            return mapper.readValue(json, type);
        } catch (IOException e) {
            logger.warn("read value from jsonstring by type ref:{} exception,class:{}", json, type);
            return null;
        }
    }

    public static <T> T parseObject(final File file, final Class<T> type) throws IOException {
        return mapper.readValue(file, type);
    }

    public static <T> T parseObject(final JsonNode jsonNode, final Class<T> type) throws IOException {
        return mapper.treeToValue(jsonNode, type);
    }

    public static JsonNode toJsonNode(final Object object) {
        return mapper.valueToTree(object);
    }

    public static String toJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String toJsonWithType(Object object) {
        try {
            return mapperWithDefaultTyping.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 判断通过 {@link #toJsonWithType(Object)} 返回的 string 有没有携带类型信息
     *
     * @param json
     * @return
     */
    public static boolean isJsonStringWithType(String json) {
        return json.startsWith("[") && json.endsWith("]");
    }
}
