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

package com.weibo.rill.flow.olympicene.core.model.event;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.context.ApplicationEvent;

import java.util.Map;

public class DAGDescriptorEvent extends ApplicationEvent {
    public enum Type {
        addDescriptor,modifyGray,modifyFunctionAB
    }

    @Getter
    @Setter
    @Builder
    public static class DAGDescriptorOperation {
        private String identity;
        private boolean add;
        private String businessId;
        private String configKey;
        private String resourceName;
        private String abRule;
        private String featureName;
        private String alias;
        private String grayRule;
        private Type type;
        private Map<String, String> attachments;
    }

    @Getter
    private DAGDescriptorOperation dagDescriptorOperation;

    public DAGDescriptorEvent(DAGDescriptorOperation source) {
        super(source);
        this.dagDescriptorOperation = source;
    }
}
