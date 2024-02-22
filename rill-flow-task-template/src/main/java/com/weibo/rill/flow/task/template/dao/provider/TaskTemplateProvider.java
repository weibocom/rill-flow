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

package com.weibo.rill.flow.task.template.dao.provider;

import com.weibo.rill.flow.task.template.dao.model.TaskTemplateDO;
import com.weibo.rill.flow.task.template.model.TaskTemplateParams;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.jdbc.SQL;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class TaskTemplateProvider {
    private static final String TABLE_NAME = "task_template";
    private static final List<String> COLUMNS = Arrays.asList("id", "name", "type", "category", "icon", "task_yaml",
            "schema", "output", "enable", "create_time", "update_time");

    @ResultType(Integer.class)
    public String insert(TaskTemplateDO taskTemplateDO) {
        SQL sql = new SQL().INSERT_INTO(TABLE_NAME);
        for (String column : COLUMNS) {
            if (!column.equals("id")) {
                sql.VALUES("`" + column + "`", "#{" + castUnderlineToCamel(column) + "}");
            }
            if (Arrays.asList("create_time", "update_time").contains(column)) {
                sql.SET("`" + column + "` = now()");
            }
        }
        return sql.toString();
    }

    /**
     * 根据 TaskTemplateDO 更新数据库
     */
    @ResultType(Integer.class)
    public String update(TaskTemplateDO taskTemplateDO) {
        if (taskTemplateDO.getId() == null) {
            throw new RuntimeException("id cannot be null when update task template");
        }
        SQL sql = new SQL().UPDATE(TABLE_NAME);
        Field[] fields = taskTemplateDO.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            if (fieldName.equals("id")) {
                continue;
            }
            try {
                field.setAccessible(true);
                Object value = field.get(taskTemplateDO);
                if (value != null) {
                    sql.SET("`" + castCamelToUnderline(fieldName) + "` = #{" + fieldName + "}");
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("get field value failed");
            }
        }
        sql.WHERE("id = #{id}");
        return sql.toString();
    }

    /**
     * 根据 TaskTemplateParams 查询 TaskTemplateDO 列表
     * @param params
     * @return
     */
    @ResultType(TaskTemplateDO.class)
    public String getTaskTemplateList(TaskTemplateParams params) {
        SQL sql = new SQL().SELECT("`" + String.join("`,`", COLUMNS) + "`").FROM(TABLE_NAME);
        if (params.getId() != null) {
            sql.WHERE("`id` = #{id}");
        }
        if (params.getType() != null) {
            sql.WHERE("`type` = #{type}");
        }
        if (params.getName() != null) {
            sql.WHERE("`name` like '%' #{name} '%'");
        }
        if (params.getCategory() != null) {
            sql.WHERE("`category` = #{category}");
        }
        if (params.getEnable() != null) {
            sql.WHERE("`enable` = #{enable}");
        }
        sql.OFFSET(params.getOffset());
        sql.LIMIT(params.getLimit());
        sql.ORDER_BY("`id` asc");
        return sql.toString();
    }


    /**
     * 将任务模板设为不可用
     * @param id 任务模板id
     * @return sql语句
     */
    @ResultType(Integer.class)
    public String disable(Long id) {
        SQL sql = new SQL().UPDATE(TABLE_NAME).SET("`enable` = 0").WHERE("`id` = #{id}");
        return sql.toString();
    }

    /**
     * 将任务模板设为启用
     * @param id 任务模板id
     * @return sql语句
     */
    @ResultType(Integer.class)
    public String enable(Long id) {
        SQL sql = new SQL().UPDATE(TABLE_NAME).SET("`enable` = 1").WHERE("`id` = #{id}");
        return sql.toString();
    }

    private String castUnderlineToCamel(String underscoreName) {
        StringBuilder result = new StringBuilder();
        boolean flag = false;
        for (int i = 0; i < underscoreName.length(); i++) {
            char ch = underscoreName.charAt(i);
            if ('_' == ch) {
                flag = true;
            } else {
                result.append(flag ? Character.toUpperCase(ch) : ch);
                flag = false;
            }
        }
        return result.toString();
    }

    /**
     * 将驼峰命名转换为下划线命名
     * @param camelCase
     * @return
     */
    private String castCamelToUnderline(String camelCase) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char currentChar = camelCase.charAt(i);

            if (Character.isUpperCase(currentChar)) {
                // 如果是大写字母，添加下划线并将字母转为小写
                result.append('_').append(Character.toLowerCase(currentChar));
            } else {
                // 如果是小写字母，直接添加
                result.append(currentChar);
            }
        }

        return result.toString();
    }
}
