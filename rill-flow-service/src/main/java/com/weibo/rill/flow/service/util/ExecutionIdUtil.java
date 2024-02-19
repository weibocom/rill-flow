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
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.common.constant.ReservedConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;


@Slf4j
public class ExecutionIdUtil {

    /**
     * 格式
     *   workspace:dagName_c_suffix
     * 概念
     *   businessId: workspace flow中一个业务只能有一个工作空间，即业务与工作空间一一对应，workspace可理解为业务id
     *   feature: dagName
     *   serviceId: workspace:dagName
     * 文档
     *   workspace与dagName说明见Olympicene与flow README文档
     */
    private static final String EXECUTION_ID_FORMAT = "%s:%s" + ReservedConstant.EXECUTION_ID_CONNECTOR + "%s";

    private static final String BUCKET_NAME_PREFIX = "bucket_";

    public static String generateExecutionId(DAG dag) {
        return String.format(EXECUTION_ID_FORMAT, dag.getWorkspace(), dag.getDagName(), UuidUtil.jobId());
    }

    public static String generateExecutionId(String businessId, String featureName) {
        return String.format(EXECUTION_ID_FORMAT, businessId, featureName, UuidUtil.jobId());
    }

    public static String generateExecutionId(String serviceId) {
        return serviceId + ReservedConstant.EXECUTION_ID_CONNECTOR + UuidUtil.jobId();
    }

    public static String changeExecutionIdToFixedSuffix(String executionId, String suffix) {
        String serviceId = getServiceId(executionId);
        return serviceId + ReservedConstant.EXECUTION_ID_CONNECTOR + suffix;
    }

    public static String generateServiceId(DAG dag) {
        return dag.getWorkspace() + ":" + dag.getDagName();
    }

    public static String getBusinessId(String key) {
        int connectorIndex = key.indexOf(ReservedConstant.EXECUTION_ID_CONNECTOR);
        if (connectorIndex < 0) {
            log.info("getBusinessId key:{} do not contains executionId", key);
            return key;
        }

        String serviceId = getServiceIdFromExecutionId(key, connectorIndex);
        return getBusinessIdFromServiceId(serviceId);
    }

    public static String getBusinessIdFromServiceId(String serviceId) {
        int colonIndex = serviceId.indexOf(ReservedConstant.COLON);
        return colonIndex < 0 ? serviceId : serviceId.substring(0, colonIndex);
    }

    public static String getServiceId(String key) {
        int connectorIndex = key.indexOf(ReservedConstant.EXECUTION_ID_CONNECTOR);
        if (connectorIndex < 0) {
            log.info("getServiceId key:{} do not contains executionId", key);
            return key;
        }

        return getServiceIdFromExecutionId(key, connectorIndex);
    }

    private static String getServiceIdFromExecutionId(String executionId, int connectorIndex) {
        List<Character> specialChar = Lists.newArrayList(':');
        int startIndex = 0;
        for (int index = connectorIndex - 1; index >= 0; index--) {
            char keyChar = executionId.charAt(index);
            if (!CharUtils.isAsciiAlphanumeric(keyChar) && !specialChar.remove((Character) keyChar)) {
                startIndex = index + 1;
                break;
            }
        }
        return executionId.substring(startIndex, connectorIndex);
    }

    public static Pair<String, String> getServiceIdAndUUIDFromExecutionId(String executionId) {
        String[] array = executionId.split(ReservedConstant.EXECUTION_ID_CONNECTOR);
        return Pair.of(array[0], array[1]);
    }

    public static long getSubmitTime(String executionId) {
        String[] array = executionId.split(ReservedConstant.EXECUTION_ID_CONNECTOR);
        return UuidUtil.timestamp(array[1]);
    }

    public static boolean isExecutionId(String source) {
        return StringUtils.isNotEmpty(source)
                && !StringUtils.containsWhitespace(source)
                && source.contains(ReservedConstant.EXECUTION_ID_CONNECTOR);
    }

    public static String generateBucketName(String executionId) {
        return BUCKET_NAME_PREFIX + executionId;
    }

    public static String getExecutionIdFromBucketName(String bucketName) {
        return bucketName.replaceFirst(BUCKET_NAME_PREFIX, StringUtils.EMPTY);
    }

    private ExecutionIdUtil() {

    }
}
