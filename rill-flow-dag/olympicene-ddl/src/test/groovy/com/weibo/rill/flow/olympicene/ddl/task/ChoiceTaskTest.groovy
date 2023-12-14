package com.weibo.rill.flow.olympicene.ddl.task


import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.ChoiceTaskValidator
import spock.lang.Specification

class ChoiceTaskTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new ChoiceTaskValidator()])])

    def "test choice task mapper"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: choice\n" +
                "  name: testName\n" +
                "  choices: \n" +
                "     - condition: \$.context.tt==1\n" +
                "       tasks: \n" +
                "         - category: function\n" +
                "           name: f1\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  next: "
        when:
        dagParser.parse(text)

        then:
        noExceptionThrown()
    }


    def "test choice task validation. choices is empty"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: choice\n" +
                "  name: testName\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  next: "
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.getErrorCode() == DDLErrorCode.CHOICE_TASK_INVALID.getCode()
    }

    def "test choice task validation. choices is empty2"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: choice\n" +
                "  name: testName\n" +
                "  choices: \n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  next: "
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.getErrorCode() == DDLErrorCode.CHOICE_TASK_INVALID.getCode()
    }

}