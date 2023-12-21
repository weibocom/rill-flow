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

    public DAGContextBuilder newCallbackContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeCallbackContextMaxSize());
    }

    public DAGContextBuilder newWakeupContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeCallbackContextMaxSize());
    }

    public DAGContextBuilder newRedoContextBuilder() {
        return new DAGContextBuilder()
                .withMaxSize(bizDConfs.getRuntimeCallbackContextMaxSize());
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
