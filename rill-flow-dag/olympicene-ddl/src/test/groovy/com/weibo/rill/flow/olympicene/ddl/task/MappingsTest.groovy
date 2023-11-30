package com.weibo.rill.flow.olympicene.ddl.task


import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLMapper
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import spock.lang.Specification

class MappingsTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])

    def "test function task mapper"() {
        given:
        String text = "category: function\n" +
                "name: normalise\n" +
                "resourceName: testBusinessId::testFeatureName::testResource::prod \n" +
                "group: split\n" +
                "pattern: task_scheduler\n" +
                "inputMappings:\n" +
                "   - target: \$.input.url\n" +
                "     source: \$.context.url\n" +
                "outputMappings:\n" +
                "   - target: \$.output.segments\n" +
                "     source: \$.context.segments\n" +
                "next: segmentForeach";
        when:
        FunctionTask ret = YAMLMapper.parseObject(text, FunctionTask.class)

        then:
        ret instanceof FunctionTask
        ret.inputMappings.size() == 1
        ret.outputMappings.size() == 1
        ret.inputMappings.get(0).source == '$.context.url'
        ret.inputMappings.get(0).target == '$.input.url'
        ret.outputMappings.get(0).source == '$.context.segments'
        ret.outputMappings.get(0).target == '$.output.segments'
    }
}