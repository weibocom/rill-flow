package com.weibo.rill.flow.olympicene.core.model.event;

import lombok.Builder;
import lombok.Data;
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
