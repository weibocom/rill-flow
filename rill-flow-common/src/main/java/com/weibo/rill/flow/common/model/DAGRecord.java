package com.weibo.rill.flow.common.model;

import lombok.Builder;
import lombok.Data;

/**
 * @author fenglin
 */
@Builder
@Data
public class DAGRecord {
    private String businessId;
    private String featureId;
    private String alia;
    private String descriptorId;
    private Long createTime;
}
