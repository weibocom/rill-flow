package com.weibo.rill.flow.task.template.model;

import lombok.Setter;

import java.util.Date;

@Setter
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
