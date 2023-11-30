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

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public class StashUtil {

    public static final String CONNECTOR = "#";
    private static final String NAME_CONF = "NAME_CONF";
    private static final String UNKNOWN_IDC = "UNKNOWN";

    private StashUtil() {
    }

    public static String buildStashTaskName(String executionId, String taskName) {
        return executionId + CONNECTOR + taskName;
    }

    public static String getExecutionId(String stashTaskName) {
        return StringUtils.substringBefore(stashTaskName, CONNECTOR);
    }

    public static String getIdc() {
        String nameConf = System.getenv(NAME_CONF);
        return getIdc(nameConf);
    }

    public static String getIdc(String nameConf) {
        return Optional.ofNullable(StringUtils.substringAfter(StringUtils.substringBefore(nameConf, "="), "-"))
                .orElse(UNKNOWN_IDC);
    }
}
