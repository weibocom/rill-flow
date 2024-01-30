package com.weibo.rill.flow.olympicene.storage.dao.mapper;

import com.weibo.rill.flow.olympicene.storage.dao.model.TaskTemplateDO;
import com.weibo.rill.flow.olympicene.storage.dao.provider.TaskTemplateProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.springframework.stereotype.Component;

@Component
public interface TaskTemplateDAO {
    @InsertProvider(type = TaskTemplateProvider.class, method = "insert")
    int insert(TaskTemplateDO taskTemplateDO);
}
