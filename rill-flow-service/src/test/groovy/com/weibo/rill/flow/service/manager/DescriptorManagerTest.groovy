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
    DescriptorManager descriptorManager = new DescriptorManager(dagParser: dagParser)

    def "test generateOutputMappings"() {
        given:
        String descriptor = "workspace: default\n" +
                "dagName: testGenerateOutputMappings\n" +
                "alias: release\n" +
                "type: flow\n" +
                "tasks:\n" +
                "  - name: functionA\n" +
                "    category: function\n" +
                "    resourceName: http://test.url\n" +
                "    requestType: POST\n" +
                "    pattern: task_sync\n" +
                "    next: functionB\n" +
                "    input:\n" +
                "      body.world: hello\n" +
                "    resourceProtocol: http\n" +
                "  - name: functionB\n" +
                "    category: function\n" +
                "    resourceName: http://test.url\n" +
                "    requestType: POST\n" +
                "    input:\n" +
                "      body.datax.y: \$.functionA.datax.y\n" +
                "      body.dataz: \$.functionA.dataz[\"a.b\"]\n" +
                "      body.datax.a: \$.functionA.datax.a\n" +
                "      body.datay.hello: \$.functionA.datay.hello\n" +
                "      body.id: \$.functionA.objs.0.id\n" +
                "      body.hello.id: \$.context.hello.objs.0.id\n" +
                "      body.world:\n" +
                "        transform: return \"hello world\";\n" +
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
            } else {
                HashSet<Mapping> inputMappings = new HashSet<>(task.getInputMappings())
                assert inputMappings.size() == 7
                assert inputMappings.contains(new Mapping("\$.context.functionA.datax.y", "\$.input.body.datax.y"))
                assert inputMappings.contains(new Mapping("\$.context.functionA.dataz[\"a.b\"]", "\$.input.body.dataz"))
                assert inputMappings.contains(new Mapping("\$.context.hello.objs.0.id", "\$.input.body.hello.id"))
                Mapping inputMapping = new Mapping();
                inputMapping.setTransform("return \"hello world\";")
                inputMapping.setTarget("\$.input.body.world")
                assert inputMappings.contains(inputMapping)
            }
        }
        println dagParser.serialize(dag)
    }

    def "test processInputOutputMappingsWhenGetDescriptor"() {
        given:
        String descriptor = "workspace: \"default\"\n" +
                "dagName: \"testGenerateOutputMappings\"\n" +
                "type: \"flow\"\n" +
                "tasks:\n" +
                "- name: \"functionA\"\n" +
                "  category: \"function\"\n" +
                "  next: \"functionB\"\n" +
                "  tolerance: false\n" +
                "  resourceName: \"http://test.url\"\n" +
                "  resourceProtocol: \"http\"\n" +
                "  pattern: \"task_sync\"\n" +
                "  inputMappings:\n" +
                "  - source: \"hello\"\n" +
                "    target: \"\$.input.body.world\"\n" +
                "  outputMappings:\n" +
                "  - source: \"\$.output.datay.hello\"\n" +
                "    target: \"\$.context.functionA.datay.hello\"\n" +
                "  - source: \"\$.output.datax\"\n" +
                "    target: \"\$.context.functionA.datax\"\n" +
                "  - source: \"\$.output.dataz['a.b']\"\n" +
                "    target: \"\$.context.functionA.dataz['a.b']\"\n" +
                "  - source: \"\$.output.objs\"\n" +
                "    target: \"\$.context.functionA.objs\"\n" +
                "  requestType: \"POST\"\n" +
                "  input:\n" +
                "    body.world: \"hello\"\n" +
                "  key_callback: false\n" +
                "- name: \"functionB\"\n" +
                "  category: \"function\"\n" +
                "  tolerance: false\n" +
                "  resourceName: \"http://test.url\"\n" +
                "  resourceProtocol: \"http\"\n" +
                "  pattern: \"task_sync\"\n" +
                "  inputMappings:\n" +
                "  - source: \"\$.context.functionA.datax.y\"\n" +
                "    target: \"\$.input.body.datax.y\"\n" +
                "  - source: \"\$.context.functionA.dataz[\\\"a.b\\\"]\"\n" +
                "    target: \"\$.input.body.dataz\"\n" +
                "  - source: \"\$.context.functionA.datax.a\"\n" +
                "    target: \"\$.input.body.datax.a\"\n" +
                "  - source: \"\$.context.functionA.datay.hello\"\n" +
                "    target: \"\$.input.body.datay.hello\"\n" +
                "  - source: \"\$.context.functionA.objs.0.id\"\n" +
                "    target: \"\$.input.body.id\"\n" +
                "  - source: \"\$.context.hello.objs.0.id\"\n" +
                "    target: \"\$.input.body.hello.id\"\n" +
                "  - transform: \"return \\\"hello world\\\";\"\n" +
                "    target: \"\$.input.body.world\"\n" +
                "  requestType: \"POST\"\n" +
                "  input:\n" +
                "    body.datax.y: \"\$.functionA.datax.y\"\n" +
                "    body.dataz: \"\$.functionA.dataz[\\\"a.b\\\"]\"\n" +
                "    body.datax.a: \"\$.functionA.datax.a\"\n" +
                "    body.datay.hello: \"\$.functionA.datay.hello\"\n" +
                "    body.id: \"\$.functionA.objs.0.id\"\n" +
                "    body.hello.id: \"\$.context.hello.objs.0.id\"\n" +
                "    body.world:\n" +
                "      transform: \"return \\\"hello world\\\";\"\n" +
                "  key_callback: false\n"
        when:
        String newDescriptor = descriptorManager.processInputOutputMappingsWhenGetDescriptor(descriptor)
        DAG newDag = dagParser.parse(newDescriptor)
        then:
        newDag.getTasks().forEach(it -> {
            assert it.inputMappings == null
            assert it.outputMappings == null
        })
    }
}
