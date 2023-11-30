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

package com.weibo.rill.flow.impl.service;

import com.weibo.rill.flow.service.service.UrlBuildService;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.util.Optional;

@Service
@Slf4j
public class UrlBuildServiceImpl implements UrlBuildService {
    @Value("${rill_flow_dag_redo_url}")
    private String redoUrl;

    @Override
    public String buildRedoUrl(String executionId, String taskNamesString) {
        URIBuilder uriBuilder;
        try {
            uriBuilder = new URIBuilder(redoUrl);
            uriBuilder.addParameter("execution_id", executionId);
            Optional.ofNullable(taskNamesString).ifPresent(it -> uriBuilder.addParameter("task_names", it));
            return uriBuilder.toString();
        } catch (URISyntaxException e) {
            log.warn("buildRedoUrl error", e);
            return redoUrl;
        }
    }
}
