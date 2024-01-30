package com.weibo.rill.flow.olympicene.storage.dao.provider;

import com.weibo.rill.flow.olympicene.storage.dao.model.TaskTemplateDO;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.jdbc.SQL;

import java.util.Arrays;
import java.util.List;

public class TaskTemplateProvider {
    private static final String TABLE_NAME = "task_template";
    private static final List<String> COLUMNS = Arrays.asList("id", "name", "type", "category", "icon", "task_yaml",
            "schema", "output", "create_time", "update_time");

    @ResultType(Integer.class)
    public String insert(TaskTemplateDO taskTemplateDO) {
        return new SQL() {
            {
                INSERT_INTO(TABLE_NAME);
                for (String column : COLUMNS) {
                    if (!column.equals("id")) {
                        VALUES(column, "#{" + "}");
                    }
                }
            }
        }.toString();
    }
}
