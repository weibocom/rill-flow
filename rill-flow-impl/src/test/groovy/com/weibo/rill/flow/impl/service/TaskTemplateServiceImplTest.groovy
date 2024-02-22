package com.weibo.rill.flow.impl.service

import com.weibo.rill.flow.olympicene.traversal.runners.AbstractTaskRunner
import com.weibo.rill.flow.task.template.dao.mapper.TaskTemplateDAO
import spock.lang.Specification
/**
 * TaskTemplateServiceImpl 测试类
 */
class TaskTemplateServiceImplTest extends Specification {
    TaskTemplateServiceImpl taskTemplateService = new TaskTemplateServiceImpl()
    AbstractTaskRunner functionTaskRunner = Mock(AbstractTaskRunner)
    AbstractTaskRunner paasTaskRunner = Mock(AbstractTaskRunner)
    Map<String, AbstractTaskRunner> taskRunnerMap = ["function": functionTaskRunner, "paas": paasTaskRunner]
    TaskTemplateDAO taskTemplateDAO = Mock(TaskTemplateDAO)

    def setup() {
        functionTaskRunner.getCategory() >> "function"
        functionTaskRunner.getIcon() >> null
        functionTaskRunner.isEnable() >> true
        paasTaskRunner.getCategory() >> "paas"
        paasTaskRunner.getIcon() >> "base64 icon code"
        paasTaskRunner.isEnable() >> false
        taskTemplateService.taskRunnerMap = taskRunnerMap
        taskTemplateService.taskTemplateDAO = taskTemplateDAO
    }
}
