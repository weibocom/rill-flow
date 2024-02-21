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

package com.weibo.rill.flow.task.template.dao.model;

import lombok.Data;

import java.util.Date;

@Data
public class TaskTemplateDO {
    private Long id;
    private String name;
    private Integer type; // 模板类型，0. 函数模板，1. 插件模板，2. 逻辑模板
    private String category;
    private String icon;
    private String taskYaml;
    private String schema;
    private Integer enable;
    private String output;
    private Date createTime;
    private Date updateTime;
}
