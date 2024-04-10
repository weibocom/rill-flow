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

import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import java.util.Map;


public interface HttpInvokeHelper {

    void appendRequestHeader(HttpHeaders httpHeaders, String executionId, TaskInfo task, Map<String, Object> input);

    HttpParameter functionRequestParams(String executionId, String taskInfoName, Resource resource, Map<String, Object> input);

    HttpParameter buildRequestParams(String executionId, Map<String, Object> input);

    String buildUrl(Resource resource, Map<String, Object> queryParams);

    String invokeRequest(String executionId, String taskInfoName, String url, HttpEntity<?> requestEntity, HttpMethod method, int maxInvokeTime);
}
