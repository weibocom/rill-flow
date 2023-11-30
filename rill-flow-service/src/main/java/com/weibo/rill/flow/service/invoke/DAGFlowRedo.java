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

package com.weibo.rill.flow.service.invoke;

import com.google.common.util.concurrent.RateLimiter;
import com.weibo.rill.flow.interfaces.http.FlowHttpClient;
import com.weibo.rill.flow.service.service.UrlBuildService;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;


@Slf4j
@Service
public class DAGFlowRedo {

    @Autowired
    private FlowHttpClient flowHttpClient;

    @Autowired
    private SwitcherManager switcherManagerImpl;

    @Autowired
    @Qualifier("multiRedoExecutor")
    private ExecutorService multiRedoExecutor;

    @Autowired
    private UrlBuildService urlBuildService;

    public void redoFlowWithTrafficLimit(List<String> executionIds, List<String> taskNames, int rate) {
        if (CollectionUtils.isEmpty(executionIds)) {
            return;
        }

        String taskNamesString = Optional.ofNullable(taskNames)
                .filter(CollectionUtils::isNotEmpty)
                .map(it -> StringUtils.join(it, ","))
                .orElse(null);
        RateLimiter rateLimiter = RateLimiter.create(rate);
        executionIds.forEach(executionId ->
                multiRedoExecutor.execute(() -> {
                    if (!switcherManagerImpl.getSwitcherState("ENABLE_FLOW_DAG_MULTI_REDO")) {
                        log.info("redoFlowWithTrafficLimit skip executionId:{}", executionId);
                        return;
                    }

                    try {
                        rateLimiter.acquire();

                        Map<String, String> headers = new HashMap<>();
                        headers.put("Content-Type", "application/json");
                        String url = urlBuildService.buildRedoUrl(executionId, taskNamesString);
                        String ret = flowHttpClient.post(url, headers, new HashMap<>());
                        log.info("redoFlowWithTrafficLimit executionId:{} ret:{}", executionId, ret);
                    } catch (Exception e) {
                        log.warn("redoFlowWithTrafficLimit fails, executionId:{}", executionId, e);
                    }
                }));
    }
}
