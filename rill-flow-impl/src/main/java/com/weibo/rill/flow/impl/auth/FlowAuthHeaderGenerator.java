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

package com.weibo.rill.flow.impl.auth;

import com.weibo.rill.flow.common.util.AuthHttpUtil;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.service.auth.AuthHeaderGenerator;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Service("authHeaderGenerator")
public class FlowAuthHeaderGenerator implements AuthHeaderGenerator {
    @Value("${rill.flow.function.trigger.uri}")
    private String flowCallbackUri;

    @Value("${rill.flow.server.host}")
    private String flowServerHost;

    @Value("${rill_flow_auth_secret_key}")
    private String authSecret;

    @Autowired
    private BizDConfs bizDConfs;

    @Override
    public void appendRequestHeader(HttpHeaders httpHeaders, String executionId, TaskInfo task, Map<String, Object> input) {
        Map<String, String> paramMap = new TreeMap<>();
        if (executionId != null) {
            paramMap.put("execution_id", executionId);
        }
        if (task != null) {
            paramMap.put("task_name", task.getName());
        }
        paramMap.put("ts", String.valueOf(System.currentTimeMillis()));
        Set<String> authBusinessWhiteList = bizDConfs.getGenerateAuthHeaderBusinessIds();
        String generateAuth = String.valueOf(input.get("generate_auth")).toLowerCase();
        if (executionId != null && authBusinessWhiteList.contains(ExecutionIdUtil.getBusinessId(executionId))
                || "1".equals(generateAuth) || "true".equals(generateAuth)
        ) {
            AuthHttpUtil.addSignToParam(paramMap, authSecret);
            input.remove("generate_auth");
        }
        httpHeaders.add("X-Flow-Server", flowServerHost);
        httpHeaders.add("X-Callback-Url", flowServerHost + flowCallbackUri + "?" + AuthHttpUtil.paramToQueryString(paramMap, "utf-8"));
    }
}
