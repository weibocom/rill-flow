package com.weibo.rill.flow.olympicene.ddl


import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode
import com.weibo.rill.flow.olympicene.ddl.exception.DDLException
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.NotSupportedTaskValidator
import spock.lang.Specification

class DAGStringParserExceptionTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new NotSupportedTaskValidator()])])

    def "test parse dag should work throw YAMLException YAML_EMPTY"() {
        given:
        String text = null

        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException)
        e.errorCode == DDLErrorCode.DAG_DESCRIPTOR_EMPTY.getCode()
        e.message == DDLErrorCode.DAG_DESCRIPTOR_EMPTY.getMessage()
    }

    def "test parse dag should work throw YAMLException YAML_EMPTY 2"() {
        given:
        String text = ""

        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException)
        e.errorCode == DDLErrorCode.DAG_DESCRIPTOR_EMPTY.getCode()
        e.message == DDLErrorCode.DAG_DESCRIPTOR_EMPTY.getMessage()
    }

    def "test parse dag should work throw YAMLException YAML_INVALID"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "tt: xxx" +
                "type: flow\n"

        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException)
        e.errorCode == DDLErrorCode.DAG_DESCRIPTOR_INVALID.getCode()
        e.message.contains('com.fasterxml.jackson.dataformat.yaml.snakeyaml.error.MarkedYAMLException')
    }

    def "test parse dag should work throw YAMLException DAG_TYPE_INVALID"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: ttt\n"

        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException)
        e.errorCode == DDLErrorCode.DAG_TYPE_INVALID.getCode()
        e.message == DDLErrorCode.DAG_TYPE_INVALID.getMessage()
    }

    def "test parse dag should work throw YAMLException TASK_EMPTY"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: flow\n"

        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException)
        e.errorCode == DDLErrorCode.DAG_TASK_EMPTY.getCode()
        e.message == DDLErrorCode.DAG_TASK_EMPTY.getMessage()
    }

    def "test next not exist in dag should throw YAMLTaskValidateException"() {
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
                "     - target: url\n" +
                "       source: url\n" +
                "     - target: url2\n" +
                "       source: url2\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  next: segment"

        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException)
        e.getErrorCode() == DDLErrorCode.TASK_NEXT_INVALID.getCode()
    }

}
