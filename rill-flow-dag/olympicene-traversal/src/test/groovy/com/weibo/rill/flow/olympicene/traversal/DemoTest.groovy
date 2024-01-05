package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import spock.lang.Specification

class DemoTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])

    def "test one functionTask dag should work well"() {
        given:
        TaskInfo finishedTaskInfo = new TaskInfo()
        finishedTaskInfo.setTaskStatus(TaskStatus.SUCCEED)
        finishedTaskInfo.setName("A")
        String text = "version: 0.0.1\n" +
                "namespace: testBusinessId\n" +
                "service: mca\n" +
                "name: testFeatureName\n" +
                "type: flow\n" +
                "tasks:\n" +
                "- category: function\n" +
                "  name: splitVideoToGop\n" +
                "  resourceName: \"testBusinessId::testFeatureName::testResource::prod\"\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$input.url\n" +
                "       source: \$context.url\n" +
                "  outputMappings:\n" +
                "     - target: \$context.inputs\n" +
                "       source: \$output.inputs\n" +
                "  next: segmentForeach\n" +
                "- category: foreach\n" +
                "  name: segmentForeach\n" +
                "  inputMappings:\n" +
                "    - target: \$input.inputs\n" +
                "      source: \$context.inputs\n" +
                "  iterationMapping: \n" +
                "      collection: \$context.inputs\n" +
                "      item: input \n" +
                "  outputMappings:\n" +
                "    - target: \$context.index\n" +
                "      source: \$output.sub_context.[*].index\n" +
                "  next: mergeGops\n" +
                "  tasks: \n" +
                "    - category: choice\n" +
                "      name: transcodeChoice\n" +
                "      inputMappings:\n" +
                "        - target: \$input.index\n" +
                "          source: \$context.input.index\n" +
                "        - target: \$input.gopType\n" +
                "          source: \$context.input.gopType\n" +
                "      outputMappings:\n" +
                "        - target: \$context.outputs\n" +
                "          source: \$output.outputs\n" +
                "      choices:\n" +
                "        - condition: \$input.gopType == \"VIDEO\"\n" +
                "          tasks:\n" +
                "            - category: function\n" +
                "              name: transcodeVideo\n" +
                "              resourceName: \"testBusinessId::testFeatureName::testResource::prod\"\n" +
                "              pattern: task_scheduler\n" +
                "              inputMappings:\n" +
                "                - target: \$input.index\n" +
                "                  source: \$temp.input.index\n" +
                "                - target: \$input.gopType\n" +
                "                  source: \$temp.input.gopType\n" +
                "              outputMappings:\n" +
                "                - target: \$temp.outputs\n" +
                "                  source: \$output.outputs\n" +
                "        - condition: \$input.gopType == \"AUDIO\"\n" +
                "          tasks:\n" +
                "            - category: function\n" +
                "              resourceName: \"testBusinessId::testFeatureName::testResource::prod\"\n" +
                "              pattern: task_scheduler\n" +
                "              name: transcodeAudio\n" +
                "              inputMappings:\n" +
                "               - target: \$input.index\n" +
                "                 source: \$temp.input.index\n" +
                "               - target: \$input.gopType\n" +
                "                 source: \$temp.input.gopType\n" +
                "              outputMappings:\n" +
                "              - target: \$temp.outputs\n" +
                "                source: \$output.outputs\n" +
                "- category: function\n" +
                "  name: mergeGops\n" +
                "  resourceName: \"testBusinessId::testFeatureName::testResource::prod\"\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "      - target: \$input.gops\n" +
                "        source: \$context.inputs\n" +
                "  outputMappings:\n" +
                "      - target: \$context.output\n" +
                "        source: \$output.output\n"

        when:
        dagParser.parse(text)

        then:
        noExceptionThrown()
    }

}