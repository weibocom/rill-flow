package com.weibo.rill.flow.olympicene.ddl

import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGType
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.NotSupportedTaskValidator
import spock.lang.Specification

class DAGStringParserTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new NotSupportedTaskValidator()])])

    def "test parse dag should work well"() {
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
                "  next: "

        when:
        DAG dag = dagParser.parse(text)

        then:
        dag instanceof DAG
        dag.workspace == 'testBusinessId'
        dag.dagName == 'mca'
        dag.type == DAGType.FLOW
    }

    def "test parse function task should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: normalise\n" +
                "  resourceName: testBusinessId::testFeatureName::testResource::prod \n" +
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

        when:
        DAG dag = dagParser.parse(text)

        then:
        dag instanceof DAG
        dag.tasks.size() == 1
        dag.tasks.get(0).name == 'normalise'
        dag.tasks.get(0).category == TaskCategory.FUNCTION.getValue()
    }

    def "test parse a 480p testFeatureName dag demo should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: flow\n" +
                "tasks:\n" +
                "  - category: function\n" +
                "    name: normalise\n" +
                "    resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "    group: split\n" +
                "    inputMappings:\n" +
                "       - target: url\n" +
                "         source: url\n" +
                "       - target: url2\n" +
                "         source: url2\n" +
                "    outputMappings:\n" +
                "       - target: segments\n" +
                "         source: segments\n" +
                "    next: segmentForeach\n" +
                "  - category: foreach\n" +
                "    name: segmentForeach\n" +
                "    inputMappings:\n" +
                "      - target: segments\n" +
                "        source: segments\n" +
                "    iterationMapping:\n" +
                "        collection: segments\n" +
                "        item: segmentUrl\n" +
                "    outputMappings:\n" +
                "      - target: gopUrls\n" +
                "        source: gopUrl\n" +
                "    next: mergeGops\n" +
                "    tasks:\n" +
                "       - category: function\n" +
                "         resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "         name: transGop\n" +
                "         inputMappings:\n" +
                "            - target: segmentUrl\n" +
                "              source: segmentUrl\n" +
                "         outputMappings:\n" +
                "            - target: gopUrl\n" +
                "              source: gopUrl\n" +
                "  - category: function\n" +
                "    name: mergeGops\n" +
                "    resourceName: \"testBusinessId::testFeatureName::testResource::prod\"\n" +
                "    group: merge\n" +
                "    inputMappings:\n" +
                "        - target: gopUrls\n" +
                "          source: gopUrls\n" +
                "    outputMappings:\n" +
                "        - target: path\n" +
                "          source: path\n" +
                "    next: remuxChoice\n" +
                "  - category: choice\n" +
                "    name: remuxChoice\n" +
                "    group: merge\n" +
                "    inputMappings:\n" +
                "      - target: path\n" +
                "        source: path\n" +
                "    outputMappings:\n" +
                "      - target: urls\n" +
                "        source: url\n" +
                "    choices: \n" +
                "        - condition: remux == \"dash\"\n" +
                "          tasks:\n" +
                "              - category: function\n" +
                "                name: dashRemux\n" +
                "                resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "                inputMappings:\n" +
                "                  - target: path\n" +
                "                    source: path\n" +
                "                outputMappings:\n" +
                "                  - target: url\n" +
                "                    source: .url\n" +
                "        - condition: remux == \"fmp4\"\n" +
                "          tasks:\n" +
                "             - category: function\n" +
                "               resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "               name: fmp4Remux\n" +
                "               inputMappings:\n" +
                "                  - target: path\n" +
                "                    source: path\n" +
                "               outputMappings:\n" +
                "                  - target: url\n" +
                "                    source: url\n" +
                "    next: callback\n" +
                "  - category: function\n" +
                "    name: callback\n" +
                "    resourceName: \"testBusinessId::testFeatureName::testResource::prod\"\n" +
                "    inputMappings:\n" +
                "    - target: urls\n" +
                "      source: urls\n" +
                "    group: merge   "

        when:
        DAG dag = dagParser.parse(text)

        then:
        dag instanceof DAG
        dag.workspace == 'testBusinessId'
        dag.dagName == 'mca'
        dag.type == DAGType.FLOW
    }
}
