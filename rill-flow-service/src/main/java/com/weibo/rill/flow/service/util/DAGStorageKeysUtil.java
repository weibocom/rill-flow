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

import com.google.common.collect.Lists;
import com.weibo.rill.flow.common.constant.ReservedConstant;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class DAGStorageKeysUtil {
    public static final String BUSINESS_ID = "business_id";
    public static final String MD5_PREFIX = "md5_";
    public static final String RELEASE = "release";
    public static final String DEFAULT = "default";

    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9]+$");
    private static final String FEATURE_KEY_RULE = "feature_%s";
    private static final String ALIAS_KEY_RULE = "alias_%s_%s";
    private static final String VERSION_KEY_RULE = "version_%s_%s_%s";
    private static final String GRAY_KEY_RULE = "gray_%s_%s";
    private static final String AB_CONFIG_KEY_RULE = "abConfigKey_%s";
    private static final String FUNCTION_AB_KEY_RULE = "functionAB_%s_%s";
    private static final String DESCRIPTOR_KEY_RULE = "descriptor_%s_%s_%s";

    private DAGStorageKeysUtil() {
    }

    public static boolean containsEmpty(String... member) {
        return member == null || Arrays.stream(member).anyMatch(StringUtils::isEmpty);
    }

    public static boolean nameInvalid(String... names) {
        return containsEmpty(names) || Arrays.stream(names).anyMatch(name -> !NAME_PATTERN.matcher(name).find());
    }

    public static String buildDescriptorRedisKey(String businessId, String featureName, String md5) {
        return String.format(DESCRIPTOR_KEY_RULE, businessId, featureName, md5);
    }

    public static String buildGrayRedisKey(String namespace, String serviceName) {
        return String.format(GRAY_KEY_RULE, namespace, serviceName);
    }

    public static String buildDescriptorId(String businessId, String featureName, String thirdPart) {
        List<String> ids = Lists.newArrayList(businessId, featureName);
        Optional.ofNullable(thirdPart).ifPresent(ids::add);
        return StringUtils.join(ids, ReservedConstant.COLON);
    }

    public static String buildVersionRedisKey(String businessId, String featureName, String alias) {
        return String.format(VERSION_KEY_RULE, businessId, featureName, alias);
    }

    public static String buildFeatureRedisKey(String businessId) {
        return String.format(FEATURE_KEY_RULE, businessId);
    }

    public static String buildAliasRedisKey(String namespace, String serviceName) {
        return String.format(ALIAS_KEY_RULE, namespace, serviceName);
    }

    public static String buildABConfigKeyRedisKey(String businessId) {
        return String.format(AB_CONFIG_KEY_RULE, businessId);
    }

    public static String buildFunctionABRedisKey(String businessId, String configKey) {
        return String.format(FUNCTION_AB_KEY_RULE, businessId, configKey);
    }
}
