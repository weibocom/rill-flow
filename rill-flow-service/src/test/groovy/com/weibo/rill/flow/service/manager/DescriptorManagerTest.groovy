package com.weibo.rill.flow.service.manager

import com.weibo.rill.flow.interfaces.model.mapping.Mapping
import com.weibo.rill.flow.interfaces.model.task.BaseTask
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.NotSupportedTaskValidator
import spock.lang.Specification

class DescriptorManagerTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new NotSupportedTaskValidator()])])
    DescriptorManager descriptorManager = new DescriptorManager()

    def "test generateOutputMappings"() {
        given:
        String descriptor = "workspace: default\n" +
                "dagName: testGenerateOutputMappings\n" +
                "alias: release\n" +
                "type: flow\n" +
                "version: v2.0\n" +
                "tasks:\n" +
                "  - name: functionA\n" +
                "    category: function\n" +
                "    resourceName: http://test.url\n" +
                "    requestType: POST\n" +
                "    pattern: task_sync\n" +
                "    next: functionB\n" +
                "    inputMappings:\n" +
                "      - source: hello\n" +
                "        target: \$.input.body.world\n" +
                "    resourceProtocol: http\n" +
                "  - name: functionB\n" +
                "    category: function\n" +
                "    resourceName: http://test.url\n" +
                "    requestType: POST\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.functionA.datax.y\n" +
                "        target: \$.input.body.datax.y\n" +
                "      - source: \$.context.functionA.dataz[\"a.b\"]\n" +
                "        target: \$.input.body.dataz\n" +
                "      - source: \$.context.functionA.datax.a\n" +
                "        target: \$.input.body.datax.a\n" +
                "      - source: \$.context.functionA.datay.hello\n" +
                "        target: \$.input.body.datay.hello\n" +
                "      - source: \$.context.functionA.objs.0.id\n" +
                "        target: \$.input.body.id\n" +
                "    resourceProtocol: http\n" +
                "    pattern: task_sync\n"
        DAG dag = dagParser.parse(descriptor)
        when:
        descriptorManager.generateOutputMappings(dag)
        then:
        dag.getTasks().size() == 2
        for (BaseTask task : dag.getTasks()) {
            if (task.getName() == "functionA") {
                Set<Mapping> outputMappings = new HashSet<>(task.getOutputMappings())
                assert outputMappings.size() == 4
                assert outputMappings.contains(new Mapping("\$.output.datax", "\$.context.functionA.datax"))
                assert outputMappings.contains(new Mapping("\$.output.datay.hello", "\$.context.functionA.datay.hello"))
                assert outputMappings.contains(new Mapping("\$.output.objs", "\$.context.functionA.objs"))
                assert outputMappings.contains(new Mapping("\$.output.dataz['a.b']", "\$.context.functionA.dataz['a.b']"))
            }
        }
    }
}
