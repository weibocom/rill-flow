package com.weibo.rill.flow.service.model;

import lombok.Data;

import java.util.Date;

@Data
public class TaskTemplate {
    private Long id;
    private String name;
    private Integer type;
    private String typeStr;
    private String nodeType;
    private String category;
    private String icon;
    private String taskYaml;
    private MetaData metaData;
    private String schema;
    private String output;
    private Integer enable;
    private Date createTime;
    private Date updateTime;
}
