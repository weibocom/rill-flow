package com.weibo.rill.flow.olympicene.ddl.task

import com.weibo.rill.flow.interfaces.model.task.FunctionPattern
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLMapper
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import spock.lang.Shared
import spock.lang.Specification

class FunctionTaskTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])

    @Shared
    String resourceNullResourceNameNull = "version: 0.0.1\n" +
            "namespace: testBusinessId\n" +
            "service: mca\n" +
            "name: testFeatureName\n" +
            "type: flow\n" +
            "tasks: \n" +
            "- category: function\n" +
            "  name: normalise\n" +
            "  pattern: task_scheduler\n" +
            "  group: split\n" +
            "  inputMappings:\n" +
            "  outputMappings:\n" +
            "  next: "
    @Shared
    String resourceNullResourceNameBlank = "version: 0.0.1\n" +
            "namespace: testBusinessId\n" +
            "service: mca\n" +
            "name: testFeatureName\n" +
            "type: flow\n" +
            "tasks: \n" +
            "- category: function\n" +
            "  name: normalise\n" +
            "  resourceName: \"\" \n" +
            "  pattern: task_scheduler\n" +
            "  group: split\n" +
            "  inputMappings:\n" +
            "  outputMappings:\n" +
            "  next: "
    @Shared
    String resourceNullResourcesNull = "version: 0.0.1\n" +
            "namespace: testBusinessId\n" +
            "service: mca\n" +
            "name: testFeatureName\n" +
            "type: flow\n" +
            "tasks: \n" +
            "- category: function\n" +
            "  name: normalise\n" +
            "  resourceName: \"resource://mock\" \n" +
            "  pattern: task_scheduler\n" +
            "  group: split\n" +
            "  inputMappings:\n" +
            "  outputMappings:\n" +
            "  next: "
    @Shared
    String resourceNameMatch = "version: 0.0.1\n" +
            "namespace: testBusinessId\n" +
            "service: mca\n" +
            "name: testFeatureName\n" +
            "type: flow\n" +
            "tasks: \n" +
            "- category: function\n" +
            "  name: normalise\n" +
            "  resourceName: \"mock\" \n" +
            "  pattern: task_scheduler\n" +
            "  group: split\n" +
            "  inputMappings:\n" +
            "  outputMappings:\n" +
            "  next: "

    def "test function task mapper"() {
        given:
        String text = "category: function\n" +
                "name: normalise\n" +
                "resourceName: testBusinessId::testFeatureName::testResource::prod \n" +
                "group: split\n" +
                "pattern: task_scheduler\n" +
                "inputMappings:\n" +
                "   - target: url\n" +
                "     source: url\n" +
                "   - target: url2\n" +
                "     source: url2\n" +
                "outputMappings:\n" +
                "   - target: segments\n" +
                "     source: segments\n" +
                "next: "
        when:
        FunctionTask ret = YAMLMapper.parseObject(text, FunctionTask.class)

        then:
        ret instanceof FunctionTask
        ret.name == 'normalise'
        ret.pattern == FunctionPattern.TASK_SCHEDULER || ret.pattern == FunctionPattern.TASK_ASYNC
        ret.resourceName == 'testBusinessId::testFeatureName::testResource::prod'
        ret.inputMappings.size() == 2
        ret.outputMappings.size() == 1
        ret.category == TaskCategory.FUNCTION.getValue()
    }

    def "function task should throw FUNTION_TASK_INVALID which because pattern is null"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: normalise\n" +
                "  resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "  group: split\n" +
                "  inputMappings:\n" +
                "  outputMappings:\n" +
                "  next: "

        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.errorCode == DDLErrorCode.FUNCTION_TASK_INVALID.getCode()
        e.message == 'function task normalise is invalid. Because pattern can not be null.'
    }

    def "function task should throw FUNTION_TASK_INVALID which because pattern is not defined"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: normalise\n" +
                "  resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "  pattern: http\n" +
                "  group: split\n" +
                "  inputMappings:\n" +
                "  outputMappings:\n" +
                "  next: "

        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.errorCode == DDLErrorCode.FUNCTION_TASK_INVALID.getCode()
        e.message == 'function task normalise is invalid. Because pattern can not be null.'
    }

    def "function task should throw FUNTION_TASK_INVALID which because resourceName can not be empty"() {
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.errorCode == DDLErrorCode.FUNCTION_TASK_INVALID.getCode()
        e.message == 'function task normalise is invalid. Because resourceName or resource can not be empty.'

        where:
        text                          | _
        resourceNullResourceNameNull  | _
        resourceNullResourceNameBlank | _
        resourceNullResourcesNull     | _
    }

    def "function task resource or resourceName test"() {
        when:
        dagParser.parse(text)

        then:
        noExceptionThrown()

        where:
        text                         | _
        resourceNameMatch            | _
    }
}