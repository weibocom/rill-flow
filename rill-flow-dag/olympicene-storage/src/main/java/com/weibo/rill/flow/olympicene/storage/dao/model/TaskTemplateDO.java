package com.weibo.rill.flow.olympicene.storage.dao.model;

import lombok.Data;

import java.util.Date;

@Data
public class TaskTemplateDO {
    private Long id;
    private String name;
    private Integer type;
    private String category;
    private String icon;
    private String taskYaml;
    private String schema;
    private String output;
    private Date createTime;
    private Date updateTime;
}
