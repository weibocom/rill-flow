package com.weibo.rill.flow.olympicene.ddl.task

import com.weibo.rill.flow.interfaces.model.task.FunctionPattern
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.olympicene.core.model.task.ChoiceTask
import com.weibo.rill.flow.olympicene.core.model.task.ForeachTask
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLMapper
import spock.lang.Specification

class TaskParseTest extends Specification {

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
                "next: segmentForeach"
        when:
        FunctionTask ret = YAMLMapper.parseObject(text, FunctionTask.class)

        then:
        ret instanceof FunctionTask
        ret.name == 'normalise'
        ret.pattern == FunctionPattern.TASK_SCHEDULER || FunctionPattern.TASK_ASYNC
        ret.resourceName == 'testBusinessId::testFeatureName::testResource::prod'
        ret.inputMappings.size() == 2
        ret.outputMappings.size() == 1
        ret.next == 'segmentForeach'
        ret.category == TaskCategory.FUNCTION.getValue()
    }

    def "test choice task mapper"() {
        given:
        String text = "category: choice\n" +
                "name: remuxChoice\n" +
                "inputMappings:\n" +
                "  - target: path\n" +
                "    source: path\n" +
                "outputMappings:\n" +
                "  - target: urls\n" +
                "    source: url\n" +
                "choices: \n" +
                "  - condition: remux == \"dash\"\n" +
                "    tasks:\n" +
                "      - category: function\n" +
                "        name: dashRemux\n" +
                "        resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "  - condition: remux == \"fmp4\"\n" +
                "    tasks:\n" +
                "      - category: function\n" +
                "        resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "        name: fmp4Remux\n" +
                "next: callback\n"
        when:
        ChoiceTask ret = YAMLMapper.parseObject(text, ChoiceTask.class)

        then:
        ret instanceof ChoiceTask
        ret.name == 'remuxChoice'
        ret.next == 'callback'
        ret.inputMappings.size() == 1
        ret.inputMappings.get(0).source == 'path'
        ret.inputMappings.get(0).target == 'path'
        ret.outputMappings.size() == 1
        ret.outputMappings.get(0).source == 'url'
        ret.outputMappings.get(0).target == 'urls'
        ret.category == TaskCategory.CHOICE.getValue()
    }

    def "test foreach task mapper"() {
        given:
        String text = "category: foreach\n" +
                "name: segmentForeach\n" +
                "inputMappings:\n" +
                "  - target: segments\n" +
                "    source: segments\n" +
                "iterationMapping:\n" +
                "    collection: segments\n" +
                "    item: segmentUrl\n" +
                "outputMappings:\n" +
                "  - target: gopUrls\n" +
                "    source: gopUrl\n" +
                "next: mergeGops\n" +
                "tasks:\n" +
                "   - category: function\n" +
                "     resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "     name: transGop\n" +
                "     inputMappings:\n" +
                "        - target: segmentUrl\n" +
                "          source: segmentUrl\n" +
                "     outputMappings:\n" +
                "        - target: gopUrl\n" +
                "          source: gopUrl\n"
        when:
        ForeachTask ret = YAMLMapper.parseObject(text, ForeachTask.class)

        then:
        ret instanceof ForeachTask
        ret.name == 'segmentForeach'
        ret.next == 'mergeGops'
        ret.inputMappings.size() == 1
        ret.inputMappings.get(0).source == 'segments'
        ret.inputMappings.get(0).target == 'segments'
        ret.outputMappings.size() == 1
        ret.outputMappings.get(0).source == 'gopUrl'
        ret.outputMappings.get(0).target == 'gopUrls'
        ret.iterationMapping.collection == 'segments'
        ret.iterationMapping.item == 'segmentUrl'
        ret.tasks.size() == 1
        ret.tasks.get(0).category == TaskCategory.FUNCTION.getValue()
        ret.category == TaskCategory.FOREACH.getValue()
    }


    def "test task enum no value mapper"() {
        given:
        String text = "category: function\n" +
                "name: normalise\n" +
                "resourceName: testBusinessId::testFeatureName::testResource::prod \n" +
                "group: split\n" +
                "pattern: ttt\n";
        when:
        FunctionTask ret = YAMLMapper.parseObject(text, FunctionTask.class)

        then:
        ret instanceof FunctionTask
        ret.pattern == null
    }
}
