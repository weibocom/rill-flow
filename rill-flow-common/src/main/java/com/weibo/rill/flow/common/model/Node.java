package com.weibo.rill.flow.common.model;

import lombok.Builder;
import lombok.Data;

/**
 * @author fenglin
 * @Description
 * @createTime 2023年09月19日 11:37:00
 */
@Builder
@Data
public class Node {
    private int id;
    private String name;
    private String category;
    private String status;
    private String resourceName;
    private String pattern;
    private String inputMappings;
    private String outputMappings;
    private String conditions;
    private String keyExp;
    private String tolerance;
    private String icon;
    private String schema;
}
