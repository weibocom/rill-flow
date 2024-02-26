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

package com.weibo.rill.flow.service.context;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class DAGContextInitializer {

    @Autowired
    private BizDConfs bizDConfs;

    @Autowired
    private List<ContextInitializeHook<Map<String, Object>>> contextInitializeHookList;

    public DAGContextBuilder newSubmitContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeSubmitContextMaxSize())
                .withHooks(contextInitializeHookList);
    }

    public DAGContextBuilder newSubmitContextBuilder(String businessId) {
        Map<String, Integer> submitContextMaxSizeMap = bizDConfs.getRedisBusinessIdToRuntimeSubmitContextMaxSize();
        if (submitContextMaxSizeMap == null || submitContextMaxSizeMap.get(businessId) == null) {
            return newSubmitContextBuilder();
        }

        return new DAGContextBuilder()
                .withMaxSize(submitContextMaxSizeMap.get(businessId))
                .withHooks(contextInitializeHookList);
    }

    public DAGContextBuilder newCallbackContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeCallbackContextMaxSize());
    }

    public DAGContextBuilder newCallbackContextBuilder(String businessId) {
        Map<String, Integer> callbackContextMaxSizeMap = bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize();
        if (callbackContextMaxSizeMap == null || callbackContextMaxSizeMap.get(businessId) == null) {
            return newCallbackContextBuilder();
        }
        return new DAGContextBuilder()
                .withMaxSize(callbackContextMaxSizeMap.get(businessId));
    }

    public DAGContextBuilder newWakeupContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeCallbackContextMaxSize());
    }

    public DAGContextBuilder newWakeupContextBuilder(String businessId) {
        Map<String, Integer> callbackContextMaxSizeMap = bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize();
        if (callbackContextMaxSizeMap == null || callbackContextMaxSizeMap.get(businessId) == null) {
            return newWakeupContextBuilder();
        }
        return new DAGContextBuilder()
                .withMaxSize(callbackContextMaxSizeMap.get(businessId));
    }

    public DAGContextBuilder newRedoContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeCallbackContextMaxSize());
    }

    public DAGContextBuilder newRedoContextBuilder(String businessId) {
        Map<String, Integer> callbackContextMaxSizeMap = bizDConfs.getRedisBusinessIdToRuntimeCallbackContextMaxSize();
        if (callbackContextMaxSizeMap == null || callbackContextMaxSizeMap.get(businessId) == null) {
            return newRedoContextBuilder();
        }
        return new DAGContextBuilder()
                .withMaxSize(callbackContextMaxSizeMap.get(businessId));
    }

    public static class DAGContextBuilder {
        private JSONObject jsonObject;
        private String identity;
        private int maxSize;
        private List<ContextInitializeHook<Map<String, Object>>> contextInitializeHookList;

        public DAGContextBuilder withData(JSONObject json) {
            this.jsonObject = json;
            return this;
        }

        public DAGContextBuilder withMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public DAGContextBuilder withIdentity(String identity) {
            this.identity = identity;
            return this;
        }

        public DAGContextBuilder withHooks(List<ContextInitializeHook<Map<String, Object>>> contextInitializeHookList) {
            this.contextInitializeHookList = contextInitializeHookList;
            return this;
        }

        public Map<String, Object> build() {
            if (jsonObject == null) {
                return new JSONObject();
            }

            if (jsonObject.toJSONString().length() > maxSize) {
                throw new TaskException(BizError.ERROR_DATA_FORMAT, identity, "context size nonsupport");
            }
            Map<String, Object> context = jsonObject;

            if (contextInitializeHookList != null) {
                for (ContextInitializeHook<Map<String, Object>> contextInitializeHook : contextInitializeHookList) {
                    context = contextInitializeHook.initialize(context);
                }
            }
            return context;
        }
    }
}
