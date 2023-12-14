package com.weibo.rill.flow.olympicene.ddl.task


import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.ForeachTaskValidator
import spock.lang.Specification

class ForeachTaskTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new ForeachTaskValidator()])])

    def "test foreach task validation. iterationMapping can not be empty"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: foreach\n" +
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
        e.getErrorCode() == DDLErrorCode.FOREACH_TASK_INVALID.getCode()
    }

    def "test foreach task validation. iterationMapping collection can not be empty"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: foreach\n" +
                "  name: testName\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  iterationMappings:\n" +
                "       collection: \n" +
                "  next: "
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.getErrorCode() == DDLErrorCode.FOREACH_TASK_INVALID.getCode()
    }

    def "test foreach task validation. tasks can not be empty"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: foreach\n" +
                "  name: testName\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  iterationMappings:\n" +
                "       collection: \n" +
                "  next: "
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.getErrorCode() == DDLErrorCode.FOREACH_TASK_INVALID.getCode()
    }


    def "test foreach task validation. no exception"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: foreach\n" +
                "  name: testName\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  iterationMapping:\n" +
                "       collection: \$.test\n" +
                "  tasks:\n" +
                "    - category: function\n" +
                "      name: testName1\n" +
                "  next: "
        when:
        dagParser.parse(text)

        then:
        noExceptionThrown()
    }

}