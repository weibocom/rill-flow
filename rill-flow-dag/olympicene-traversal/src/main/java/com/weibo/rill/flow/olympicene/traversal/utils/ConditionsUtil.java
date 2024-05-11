package com.weibo.rill.flow.olympicene.traversal.utils;

import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

public class ConditionsUtil {
    private ConditionsUtil() { }


    public static final Configuration valuePathConf = Configuration.builder()
            .options(Option.SUPPRESS_EXCEPTIONS)
            .options(Option.AS_PATH_LIST)
            .build();

    public static boolean conditionsAllMatch(List<String> conditions, Map<String, Object> valueMap, String mapType) {
        return conditions.stream()
                .map(condition -> JsonPath.using(valuePathConf).parse(ImmutableMap.of(mapType, valueMap)).read(condition))
                .allMatch(it -> org.apache.commons.collections.CollectionUtils.isNotEmpty((List<Object>) it));
    }

    public static boolean conditionsAnyMatch(List<String> conditions, Map<String, Object> valueMap, String mapType) {
        return conditions.stream()
                .map(condition -> JsonPath.using(valuePathConf).parse(ImmutableMap.of(mapType, valueMap)).read(condition))
                .anyMatch(it -> CollectionUtils.isNotEmpty((List<Object>) it));
    }
}
