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

package com.weibo.rill.flow.olympicene.traversal.mappings;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.jayway.jsonpath.*;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JSONPathInputOutputMapping implements InputOutputMapping, JSONPath {
    Configuration conf = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();

    @Override
    public void mapping(Map<String, Object> context, Map<String, Object> input, Map<String, Object> output, List<Mapping> rules) {
        if (CollectionUtils.isEmpty(rules) || context == null || input == null || output == null) {
            return;
        }

        Map<String, Object> map = new HashMap<>();
        map.put("context", context);
        map.put("input", input);
        map.put("output", output);

        List<Mapping> mappingRules = rules.stream()
                .filter(rule -> StringUtils.isNoneBlank(rule.getSource()) && StringUtils.isNoneBlank(rule.getTarget()))
                .toList();
        for (Mapping mapping : mappingRules) {
            boolean intolerance = mapping.getTolerance() != null && !mapping.getTolerance();
            try {
                String source = mapping.getSource();
                Object sourceValue = source.startsWith("$") ? getValue(map, source) : parseSource(source);

                Object transformedValue = transformSourceValue(sourceValue, context, input, output, mapping.getTransform());

                if (transformedValue != null) {
                    map = setValue(map, transformedValue, mapping.getTarget());
                }
            } catch (Exception e) {
                log.warn("mapping fails, intolerance:{}, mapping:{} due to {}", intolerance, mapping, e.getMessage());
                if (intolerance) {
                    throw e;
                }
            }
        }
    }

    public Object transformSourceValue(Object sourceValue, Map<String, Object> context, Map<String, Object> input,
                                        Map<String, Object> output, String transform) {
        if (StringUtils.isBlank(transform)) {
            return sourceValue;
        }

        Map<String, Object> env = Maps.newHashMap();
        env.put("source", sourceValue);
        env.put("context", context);
        env.put("input", input);
        env.put("output", output);
        return doTransform(transform, env);
    }

    /**
     * <pre>
     * AviatorEvaluator.execute每次运行时会加载临时类
     *   如：[Loaded Script_1638847124088_67/93314457 from com.googlecode.aviator.Expression]
     * 长时间大量使用aviator会导致Metaspace oom
     *   如：java.lang.OutOfMemoryError: Compressed class space
     * 若长期大量使用aviator转换sourceValue 则建议使用guava等本地缓存框架缓存aviator表达式编译结果
     *   如：Expression expression = AviatorEvaluator.compile(transform) 缓存该表达式
     *      expression.execute(env)
     * </pre>
     */
    public Object doTransform(String transform, Map<String, Object> env) {
        return AviatorEvaluator.execute(transform, env);
    }

    public static Object parseSource(String source) {
        if (StringUtils.isBlank(source)) {
            return source;
        }

        String sourceTrim = source.trim();
        try {
            JsonNode jsonNode = DAGTraversalSerializer.MAPPER.readTree(sourceTrim);
            if (jsonNode != null && jsonNode.isObject()) {
                return DAGTraversalSerializer.MAPPER.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
                });
            }
            if (jsonNode != null && jsonNode.isArray()) {
                return DAGTraversalSerializer.MAPPER.convertValue(jsonNode, new TypeReference<List<Object>>() {
                });
            }
        } catch (Exception e) {
            // not json ignore
        }

        String sourceLowerCase = sourceTrim.toLowerCase();
        if ("true".equals(sourceLowerCase)) {
            return true;
        }
        if ("false".equals(sourceLowerCase)) {
            return false;
        }

        try {
            BigDecimal bigDecimal = new BigDecimal(sourceTrim);
            if (sourceTrim.contains(".")) {
                return bigDecimal.doubleValue();
            } else {
                return bigDecimal.longValue();
            }
        } catch (Exception e) {
            // not number ignore
        }

        return source;
    }

    @Override
    public Object getValue(Map<String, Object> map, String path) {
        try {
            return JsonPath.using(conf).parse(map).read(path);
        } catch (InvalidPathException e) {
            return null;
        }
    }

    @Override
    public Map<String, Object> setValue(Map<String, Object> map, Object value, String path) {
        if (map == null) {
            return null;
        }

        List<String> intermediateRoute = Lists.newArrayList();
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '.') {
                intermediateRoute.add(path.substring(0, i));
            }
        }

        DocumentContext context = JsonPath.using(conf).parse(map);
        for (String route : intermediateRoute) {
            if (context.read(route) == null) {
                context.set(route, new HashMap<>());
            }
        }
        return JsonPath.using(conf).parse(map).set(path, value).json();
    }

    @Override
    public Map<String, Map<String, Object>> delete(Map<String, Map<String, Object>> map, String path) {
        if (map == null) {
            return null;
        }

        return JsonPath.using(conf).parse(map).delete(path).json();
    }
}
