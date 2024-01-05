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

package com.weibo.rill.flow.service.statistic;

import com.weibo.rill.flow.service.util.ProfileActions;
import com.weibo.rill.flow.service.util.PrometheusActions;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Supplier;

@Service
public class ProfileRecordService {
    /**
     * 按业务类型分别统计 接口调用情况 运维配置的监控为接口总体调用情况
     */
    public Map<String, Object> runNotifyAndRecordProfile(String url, String id, Supplier<Map<String, Object>> notifyActions) {
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> ret = notifyActions.get();
            ProfileActions.recordHttpExecution(url, id, true, System.currentTimeMillis() - startTime);
            // 记录prometheus
            PrometheusActions.recordHttpExecution(url, id, true, System.currentTimeMillis() - startTime);
            return ret;
        } catch (Exception e) {
            ProfileActions.recordHttpExecution(url, id, false, System.currentTimeMillis() - startTime);
            // 记录prometheus
            PrometheusActions.recordHttpExecution(url, id, false, System.currentTimeMillis() - startTime);
            throw e;
        }
    }
}
