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

package com.weibo.rill.flow.olympicene.ddl.serialize;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.NullSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.core.model.task.*;
import net.sf.json.JSONNull;

import java.text.SimpleDateFormat;

public class ObjectMapperFactory {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    static {
        JSON_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JSON_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        JSON_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        addNetSfJSONNullSerializer(JSON_MAPPER);
        JSON_MAPPER.registerSubtypes(
                new NamedType(FunctionTask.class, "function"),
                new NamedType(ChoiceTask.class, "choice"),
                new NamedType(ForeachTask.class, "foreach"),
                new NamedType(PassTask.class, "pass"),
                new NamedType(SuspenseTask.class, "suspense"),
                new NamedType(ReturnTask.class, "return"),
                new NamedType(SwitchTask.class, "switch")
        );
    }

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
            .configure(YAMLGenerator.Feature.USE_NATIVE_OBJECT_ID, false);
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(YAML_FACTORY);

    static {
        YAML_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        YAML_MAPPER.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
        YAML_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        YAML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        YAML_MAPPER.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        YAML_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        YAML_MAPPER.registerSubtypes(
                new NamedType(FunctionTask.class, "function"),
                new NamedType(ChoiceTask.class, "choice"),
                new NamedType(ForeachTask.class, "foreach"),
                new NamedType(PassTask.class, "pass"),
                new NamedType(SuspenseTask.class, "suspense"),
                new NamedType(ReturnTask.class, "return"),
                new NamedType(SwitchTask.class, "switch")
        );
    }

    private ObjectMapperFactory() {
    }

    public static ObjectMapper getJSONMapper() {
        return JSON_MAPPER;
    }

    public static ObjectMapper getYamlMapper() {
        return YAML_MAPPER;
    }

    public static void registerSubtypes(NamedType... types) {
        if (types == null) {
            return;
        }
        JSON_MAPPER.registerSubtypes(types);
        YAML_MAPPER.registerSubtypes(types);
    }

    public static ObjectMapper createMvcObjectMapper() {
        return JSON_MAPPER;
    }

    public static void addNetSfJSONNullSerializer(ObjectMapper mapper) {
        //下面三行解决net.sf.json.JSONNull反序列化异常的问题
        SimpleModule netSfJsonModule = new SimpleModule("net.sf.json");
        netSfJsonModule.addSerializer(JSONNull.class, NullSerializer.instance);
        mapper.registerModule(netSfJsonModule);
    }
}
