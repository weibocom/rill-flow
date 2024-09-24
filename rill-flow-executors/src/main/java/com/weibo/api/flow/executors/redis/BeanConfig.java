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

package com.weibo.api.flow.executors.redis;

import lombok.Data;


@Data
public class BeanConfig {
    private Redis redis;
    private Http http;
    private Submit submit;

    @Data
    public static class Redis {
        private String master;
        private String slave;
        private String port;
        private Boolean threadIsolation;
    }

    @Data
    public static class Http {
        private Integer conTimeOutMs;
        private Integer writeTimeoutMs;
        private Integer readTimeoutMs;
        private Integer maxIdleConnections;
        private Integer keepAliveDurationMs;
        private Integer retryTimes;
    }

    @Data
    public static class Submit {
        private Double completeRateThreshold;
        private Double successRateThreshold;
        private Integer heapThreshold;
    }
}
