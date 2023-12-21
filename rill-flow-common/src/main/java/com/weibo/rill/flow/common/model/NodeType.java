package com.weibo.rill.flow.common.model;


/**
 * @author fenglin
 */
public enum NodeType {
    FUNCTION("function"),
    HTTP_HTTPS("http/https"),
    RILL_FLOW("rillflow"),
    AB("ab"),
    FOREACH("foreach"),
    RETURN("return")
    ;
    private final String type;

    NodeType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
