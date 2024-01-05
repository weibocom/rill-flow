package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import spock.lang.Specification

class ChoiceWithForeachTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator()])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    /**
     * A -> B -> C
     * B为foreach:
     * B1 -> B2 -> B3
     * B2为choice
     * condition1: B21  run well
     * condition2: B22  skipped
     *
     * @return
     */
    def "test two FunctionTask and one ForeachTask with two ChoiceTask dag should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: \"olympicene::test::function1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "     - target: \$.context.status\n" +
                "       source: \$.output.status\n" +
                "  next: B\n" +
                "- category: foreach\n" +
                "  name: B\n" +
                "  next: C\n" +
                "  inputMappings:\n" +
                "    - target: \$.input.segments\n" +
                "      source: \$.context.segments\n" +
                "    - target: \$.input.status\n" +
                "      source: \$.context.status\n" +
                "  outputMappings:\n" +
                "    - target: \$.context.gopUrls\n" +
                "      source: \$.output.sub_context.[*].gopUrl\n" +
                "  iterationMapping:\n" +
                "      collection: \$.input.segments\n" +
                "      item: segmentUrl\n" +
                "  tasks:\n" +
                "     - category: function\n" +
                "       resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "       pattern: task_scheduler\n" +
                "       name: B1\n" +
                "       next: B2\n" +
                "       inputMappings:\n" +
                "          - target: \$.input.segmentUrl\n" +
                "            source: \$.context.segmentUrl\n" +
                "       outputMappings:\n" +
                "          - target: \$.context.gopPath\n" +
                "            source: \$.output.gopPath\n" +
                "     - category: choice\n" +
                "       name: B2\n" +
                "       next: B3\n" +
                "       inputMappings:\n" +
                "          - target: \$.input.gopPath\n" +
                "            source: \$.context.gopPath\n" +
                "          - target: \$.input.status\n" +
                "            source: \$.context.status\n" +
                "       outputMappings:\n" +
                "          - target: \$.context.gopPath\n" +
                "            source: \$.output.gopPath\n" +
                "       choices:\n" +
                "          - condition: \$.input.[?(@.status == \"succeed\")]\n" +
                "            tasks: \n" +
                "              - category: function\n" +
                "                resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "                pattern: task_scheduler\n" +
                "                name: B21\n" +
                "                inputMappings:\n" +
                "                   - target: \$.input.gopPath\n" +
                "                     source: \$.context.gopPath\n" +
                "                outputMappings:\n" +
                "                   - target: \$.context.gopPath\n" +
                "                     source: \$.output.gopPath\n" +
                "          - condition: \$.input.[?(@.status == \"failed\")]\n" +
                "            tasks: \n" +
                "              - category: function\n" +
                "                resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "                pattern: task_scheduler\n" +
                "                name: B22\n" +
                "                inputMappings:\n" +
                "                   - target: \$.input.gopPath\n" +
                "                     source: \$.context.gopPath\n" +
                "                outputMappings:\n" +
                "                   - target: \$.context.gopPath\n" +
                "                     source: \$.output.gopPath\n" +
                "     - category: function\n" +
                "       resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "       name: B3\n" +
                "       pattern: task_scheduler\n" +
                "       inputMappings:\n" +
                "          - target: \$.input.gopPath\n" +
                "            source: \$.context.gopPath\n" +
                "       outputMappings:\n" +
                "          - target: \$.context.gopUrl\n" +
                "            source: \$.output.gopUrl\n" +
                "- category: function\n" +
                "  name: C\n" +
                "  resourceName: \"olympicene::test::function1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.result\n" +
                "       source: \$.output.result\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('xxx1', dag, ["url": "http://test.com/test"])

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['status': 'succeed', 'segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopPath': '/donghai/path1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B1")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopPath': '/donghai/path2'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B1")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopPath': '/donghai/new/path1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B2_1-B21")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopPath': '/donghai/new/path2'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B2_1-B21")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'http://gopUrl1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B3")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'http://gopUrl2'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B3")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['result': 'http://test.com/result'],
                NotifyInfo.builder()
                        .taskInfoName("C")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/test', 'result': 'http://test.com/result', 'gopUrls': ['http://gopUrl2', 'http://gopUrl1'], 'status': 'succeed', 'segments': ['gopUrl1', 'gopUrl2']]
        })
        11 * callback.onEvent({ Event event -> event.eventCode == DAGEvent.TASK_FINISH.getCode() })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'A' && it.input == ['url': 'http://test.com/test'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B1' && it.input == ['segmentUrl': 'gopUrl1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B1' && it.input == ['segmentUrl': 'gopUrl2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B2_1-B21' && it.input == ['gopPath': '/donghai/path1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B2_1-B21' && it.input == ['gopPath': '/donghai/path2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B3' && it.input == ['gopPath': '/donghai/new/path1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B3' && it.input == ['gopPath': '/donghai/new/path2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'C' && it.input == ['gopUrls': ['http://gopUrl2', 'http://gopUrl1']] })
    }
}