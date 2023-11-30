package com.weibo.rill.flow.common.model;


/**
 * @author fenglin
 * @Description
 * @createTime 2023年09月19日 11:43:00
 */
public enum NodeType {
    FUNCTION("function"),
    HTTP_HTTPS("http/https"),
    RILL_FLOW("rillflow"),
    AB("ab"),
    FOREACH("foreach"),
    RETURN("return")
    ;
    private String type;

    NodeType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
