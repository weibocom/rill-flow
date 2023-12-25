package com.weibo.rill.flow.olympicene.ddl.validate

import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode
import com.weibo.rill.flow.olympicene.ddl.exception.DDLException
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.BaseTaskValidator
import spock.lang.Specification

class BaseTaskValidatorTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new BaseTaskValidator()])])

    def "test base task validate. name can not be null"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
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
                "  next: segmentForeach"
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.errorCode == DDLErrorCode.NAME_INVALID.getCode()
    }

    def "test base task validate. name can not be empty"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "  name: \n" +
                "  group: split\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "     - target: url2\n" +
                "       source: url2\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  next: segmentForeach"
        when:
        dagParser.parse(text)

        then:
        def e = thrown(ValidationException)
        e.errorCode == DDLErrorCode.NAME_INVALID.getCode()
    }

    def "test base task validate. category can not be null"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testNS\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: functionXX\n" +
                "  resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "  name: \n" +
                "  group: split\n" +
                "  inputMappings:\n" +
                "     - target: url\n" +
                "       source: url\n" +
                "     - target: url2\n" +
                "       source: url2\n" +
                "  outputMappings:\n" +
                "     - target: segments\n" +
                "       source: segments\n" +
                "  next: segmentForeach"
        when:
        dagParser.parse(text)

        then:
        def e = thrown(DDLException.class)
        e.errorCode == DDLErrorCode.DAG_DESCRIPTOR_INVALID.getCode()
    }

}