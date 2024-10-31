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

package com.weibo.rill.flow.service.service;

import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.service.storage.dao.DAGABTestDAO;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class FunctionTaskService {
    @Autowired
    private DAGABTestDAO dagABTestDAO;
    @Autowired
    private DAGDescriptorService dagDescriptorService;

    public BaseResource getTaskResource(Long uid, Map<String, Object> input, String resourceName) {
        try {
            URI uri = new URI(resourceName);

            String dagDescriptorId = uri.getAuthority();
            // 调用量比较大 useCache=tre 以减轻redis数据获取压力
            DAG dag = dagDescriptorService.getDAG(uid, input, dagDescriptorId, true);
            if (CollectionUtils.isEmpty(dag.getResources())) {
                throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "dag resources empty");
            }

            Map<String, BaseResource> resourceMap = dag.getResources().stream()
                    .collect(Collectors.toMap(BaseResource::getName, it -> it));
            Map<String, String> queryParams = new URIBuilder(uri).getQueryParams().stream()
                    .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue, (v1, v2) -> v1));
            BaseResource baseResource = resourceMap.get(queryParams.get("name"));
            if (baseResource == null) {
                throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "dag resource null");
            }
            return baseResource;
        } catch (TaskException e) {
            throw e;
        } catch (Exception e) {
            log.warn("getTaskResource form dag config fails, resourceName:{}", resourceName, e);
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), "getTaskResource fails: " + e.getMessage(), e.getCause());
        }
    }

    public String calculateResourceName(Long uid, Map<String, Object> input, String executionId, String configKey) {
        String businessId = ExecutionIdUtil.getBusinessId(executionId);
        Pair<String, Map<String, String>> functionAB = dagABTestDAO.getFunctionAB(businessId, configKey);
        if (functionAB == null) {
            return null;
        }
        String resourceName = dagDescriptorService.getValueFromRuleMap(uid, input, functionAB.getRight(), functionAB.getLeft());
        log.info("calculateResourceName result resourceName:{} executionId:{} configKey:{}", resourceName, executionId, configKey);
        return resourceName;
    }
}
