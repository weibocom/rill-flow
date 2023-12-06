package com.weibo.rill.flow.common.model;

import com.alibaba.fastjson.JSONObject;
import lombok.Builder;
import lombok.Data;

/**
 * @author fenglin
 */
@Builder
@Data
public class DAGDetail {
    private String workspace;
    private String dagName;
    private String type;
    private String version;
    private JSONObject commonMapping;
    private JSONObject defaultContext;
    private String status;
    private String dagData;
    private int id;
}
