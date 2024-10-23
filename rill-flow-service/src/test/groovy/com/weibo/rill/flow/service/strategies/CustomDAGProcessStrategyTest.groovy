package com.weibo.rill.flow.service.strategies

import com.weibo.rill.flow.interfaces.model.mapping.Mapping
import com.weibo.rill.flow.interfaces.model.task.BaseTask
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.task.PassTask
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.NotSupportedTaskValidator
import org.junit.platform.commons.util.StringUtils
import spock.lang.Specification

class CustomDAGProcessStrategyTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new NotSupportedTaskValidator()])])
    CustomDAGProcessStrategy strategy = new CustomDAGProcessStrategy(dagParser: dagParser)

    /**
     * 测试常规 input 的解析与 inputMappings 及 outputMappings 的生成
     */
    def "test onStorage"() {
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
                "      body.datax.y.z: \$.functionA.datax.y.z\n" +
                "      body.dataz: \$.functionA.dataz[\"a.b\"]\n" +
                "      body.datax.a: \$.functionA.datax.a\n" +
                "      body.datay.hello: \$.functionA.datay.hello\n" +
                "      body.id: \$.functionA.objs[0].id\n" +
                "      body.x: \$.functionA.objs[1].x\n" +
                "      body.hello.id: \$.context.hello.objs[0].id\n" +
                "      body.world:\n" +
                "        transform: return \"hello world\";\n" +
                "    resourceProtocol: http\n" +
                "    pattern: task_sync\n"
        DAG dag = dagParser.parse(descriptor)
        when:
        strategy.onStorage(dag)
        then:
        assert dag.getTasks().size() == 2
        for (BaseTask task : dag.getTasks()) {
            if (task.getName() == "functionA") {
                Set<Mapping> outputMappings = new HashSet<>(task.getOutputMappings())
                assert outputMappings.size() == 7
                assert outputMappings.contains(new Mapping("\$.output.datax.a", "\$.context.functionA.datax.a"))
                assert outputMappings.contains(new Mapping("\$.output.datax.y", "\$.context.functionA.datax.y"))
                assert outputMappings.contains(new Mapping("\$.output.datay.hello", "\$.context.functionA.datay.hello"))
                assert outputMappings.contains(new Mapping("\$.output.objs[0].id", "\$.context.functionA.objs[0].id"))
                assert outputMappings.contains(new Mapping("\$.output.objs[1].x", "\$.context.functionA.objs[1].x"))
                assert outputMappings.contains(new Mapping("\$.output.dataz['a.b']", "\$.context.functionA.dataz['a.b']"))
            } else {
                HashSet<Mapping> inputMappings = new HashSet<>(task.getInputMappings())
                assert inputMappings.size() == 9
                assert inputMappings.contains(new Mapping("\$.context.functionA.datax.y", "\$.input.body.datax.y"))
                assert inputMappings.contains(new Mapping("\$.context.functionA.datax.y.z", "\$.input.body.datax.y.z"))
                assert inputMappings.contains(new Mapping("\$.context.functionA.dataz[\"a.b\"]", "\$.input.body.dataz"))
                assert inputMappings.contains(new Mapping("\$.context.hello.objs[0].id", "\$.input.body.hello.id"))
                assert inputMappings.contains(new Mapping("\$.context.functionA.objs[1].x", "\$.input.body.x"))
                Mapping inputMapping = new Mapping();
                inputMapping.setTransform("return \"hello world\";")
                inputMapping.setTarget("\$.input.body.world")
                assert inputMappings.contains(inputMapping)
            }
        }
    }

    /**
     * 生成 jsonpath 包含数组情况的 inputMappings 与 outputMappings 的生成
     */
    def "test onRetrieval when include *"() {
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
                "      body.elements: \$.functionA.elements.*.name\n" +
                "      body.first_id: \$.functionA.elements[0].id\n" +
                "    resourceProtocol: http\n" +
                "    pattern: task_sync\n"
        DAG dag = dagParser.parse(descriptor)
        when:
        strategy.onStorage(dag)
        then:
        assert dag.getTasks().size() == 2
        for (BaseTask task : dag.getTasks()) {
            if (task.getName() == "functionA") {
                Set<Mapping> outputMappings = new HashSet<>(task.getOutputMappings())
                assert outputMappings.size() == 2
                assert outputMappings.contains(new Mapping("\$.output.elements[*].name", "\$.context.functionA.elements[*].name"))
                assert outputMappings.contains(new Mapping("\$.output.elements[0].id", "\$.context.functionA.elements[0].id"))
            } else {
                HashSet<Mapping> inputMappings = new HashSet<>(task.getInputMappings())
                inputMappings.size() == 2
                assert inputMappings.contains(new Mapping("\$.context.functionA.elements.*.name", "\$.input.body.elements"))
                assert inputMappings.contains(new Mapping("\$.context.functionA.elements[0].id", "\$.input.body.first_id"))
            }
        }
    }

    /**
     * 测试 outputMappings 已经存在指定项的情况
     */
    def "test onRetrieval when target exists"() {
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
                "    outputMappings:\n" +
                "      - source: \$.output.id\n" +
                "        target: \$.context.functionA.id\n" +
                "  - name: functionB\n" +
                "    category: function\n" +
                "    resourceName: http://test.url\n" +
                "    requestType: POST\n" +
                "    input:\n" +
                "      body.functionA.id: \$.functionA.id\n" +
                "    resourceProtocol: http\n" +
                "    pattern: task_sync\n"
        DAG dag = dagParser.parse(descriptor)
        when:
        strategy.onStorage(dag)
        then:
        assert dag.getTasks().size() == 2
        for (BaseTask task : dag.getTasks()) {
            if (task.getName() == "functionA") {
                Set<Mapping> outputMappings = new HashSet<>(task.getOutputMappings())
                assert outputMappings.size() == 1
                assert outputMappings.contains(new Mapping("\$.output.id", "\$.context.functionA.id"))
            } else {
                HashSet<Mapping> inputMappings = new HashSet<>(task.getInputMappings())
                inputMappings.size() == 1
                assert inputMappings.contains(new Mapping("\$.context.functionA.id", "\$.input.body.functionA.id"))
            }
        }

    }

    /**
     * 测试下发时处理 descriptor 的情况
     */
    def testonRetrieval() {
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
                "      transform: \"return \\\"hello world\\\";\"\n"
        when:
        String newDescriptor = strategy.onRetrieval(descriptor)
        DAG newDag = dagParser.parse(newDescriptor)
        then:
        newDag.getTasks().forEach(it -> {
            assert it.inputMappings == null
            assert it.outputMappings == null
        })
    }

    /**
     * 测试 dag 输出节点的生成以及对应 outputMappings 的生成
     */
    def "test dag output"() {
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
                "    pattern: task_sync\n" +
                "output:\n" +
                "  end.x: \$.functionB.output.x\n" +
                "  end.y: \$.functionB.output.y\n" +
                "  end.as: \$.functionA.objs.*\n"
        DAG dag = dagParser.parse(descriptor)
        when:
        strategy.onStorage(dag)
        then:
        assert dag.getTasks().size() == 3
        assert StringUtils.isNotBlank(dag.getEndTaskName())
        dag.getTasks().forEach {task -> {
            Set<Mapping> outputMappings = new HashSet<>(task.getOutputMappings())
            if (task.getName().equals("functionA")) {
                assert outputMappings.size() == 6
                assert outputMappings.contains(new Mapping("\$.output.objs[*]", "\$.context.functionA.objs[*]"))
                assert task.next.equals("functionB," + dag.getEndTaskName()) || task.next.equals(dag.getEndTaskName() + ",functionB")
            } else if (task.getName().equals("functionB")) {
                assert outputMappings.size() == 2
                assert outputMappings.contains(new Mapping("\$.output.output.x", "\$.context.functionB.output.x"))
                assert outputMappings.contains(new Mapping("\$.output.output.y", "\$.context.functionB.output.y"))
                assert task.getNext().equals(dag.getEndTaskName())
            } else {
                assert task instanceof PassTask
                assert task.getName() == dag.getEndTaskName()
                Set<Mapping> inputMappings = new HashSet<>(task.getInputMappings())
                assert inputMappings.size() == 3
                assert inputMappings.contains(new Mapping("\$.context.functionB.output.x", "\$.input.end.x"))
                assert inputMappings.contains(new Mapping("\$.context.functionB.output.y", "\$.input.end.y"))
                assert inputMappings.contains(new Mapping("\$.context.functionA.objs.*", "\$.input.end.as"))
                assert outputMappings.size() == 3
                assert outputMappings.contains(new Mapping("\$.input.end.x", "\$.context.end.x"))
                assert outputMappings.contains(new Mapping("\$.input.end.y", "\$.context.end.y"))
                assert outputMappings.contains(new Mapping("\$.input.end.as", "\$.context.end.as"))
            }
        }}
    }

    /**
     * 测试 dag 包含 output 时 descriptor 的下发
     */
    def "test get descriptor with output"() {
        given:
        String descriptor = "workspace: \"default\"\n" +
                "dagName: \"testGenerateOutputMappings\"\n" +
                "type: \"flow\"\n" +
                "tasks:\n" +
                "- !<function>\n" +
                "  name: \"functionA\"\n" +
                "  category: \"function\"\n" +
                "  next: \"functionB,endPassTask20241018\"\n" +
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
                "- !<function>\n" +
                "  name: \"functionB\"\n" +
                "  category: \"function\"\n" +
                "  next: \"endPassTask20241018\"\n" +
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
                "  outputMappings:\n" +
                "  - source: \"\$.output.output\"\n" +
                "    target: \"\$.context.functionB.output\"\n" +
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
                "  key_callback: false\n" +
                "- !<pass>\n" +
                "  name: \"endPassTask20241018\"\n" +
                "  category: \"pass\"\n" +
                "  inputMappings:\n" +
                "  - source: \"\$.context.functionB.output.x\"\n" +
                "    target: \"\$.input.end.x\"\n" +
                "  - source: \"\$.context.functionB.output.y\"\n" +
                "    target: \"\$.input.end.y\"\n" +
                "  - source: \"\$.context.functionA.objs.*\"\n" +
                "    target: \"\$.input.end.as\"\n" +
                "  outputMappings:\n" +
                "  - source: \"\$.input.end.x\"\n" +
                "    target: \"\$.context.end.x\"\n" +
                "  - source: \"\$.input.end.y\"\n" +
                "    target: \"\$.context.end.y\"\n" +
                "  - source: \"\$.input.end.as\"\n" +
                "    target: \"\$.context.end.as\"\n" +
                "  input:\n" +
                "    end.x: \"\$.functionB.output.x\"\n" +
                "    end.y: \"\$.functionB.output.y\"\n" +
                "    end.as: \"\$.functionA.objs.*\"\n" +
                "  tolerance: false\n" +
                "  key_callback: false\n" +
                "output:\n" +
                "  end.x: \"\$.functionB.output.x\"\n" +
                "  end.y: \"\$.functionB.output.y\"\n" +
                "  end.as: \"\$.functionA.objs.*\"\n" +
                "end_task_name: \"endPassTask20241018\"\n"
        when:
        String resultDescriptor = strategy.onRetrieval(descriptor)
        DAG dag = dagParser.parse(resultDescriptor)
        then:
        assert dag.tasks.size() == 2
        assert dag.getEndTaskName() == null
        dag.tasks.forEach {task -> {
            assert task.getInputMappings() == null
            assert task.getOutputMappings() == null
        }}
    }
}
