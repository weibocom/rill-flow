package com.weibo.rill.flow.olympicene.ddl

import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.ResourceDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.NotSupportedTaskValidator
import spock.lang.Specification

class DAGSerializationTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new NotSupportedTaskValidator()]), new ResourceDAGValidator()])

    def "test parse dag and serialization should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "workspace: rillflow\n" +
                "namespace: rillflowMca\n" +
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
                "  next: "
        DAG dag = dagParser.parse(text)
        String result = dagParser.serialize(dag)

        when:
        DAG dag2 = dagParser.parse(result)

        then:
        noExceptionThrown()
        dag2.workspace == 'rillflow'
        dag2.dagName == 'mca'
    }
}