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

class ChoiceTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator()])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)
    /**
     * A -> B -> C
     * B为choice:
     * condition1: B1  run well
     * condition2: B2  skipped
     *
     * @return
     */
    def "test one ChoiceTask with two FunctionTask dag should work well"() {
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
                "     - target: \$.context.status\n" +
                "       source: \$.output.status\n" +
                "     - target: \$.context.path\n" +
                "       source: \$.output.path\n" +
                "  next: B\n" +
                "- category: choice\n" +
                "  name: B\n" +
                "  next: C\n" +
                "  inputMappings:\n" +
                "    - target: \$.input.status\n" +
                "      source: \$.context.status\n" +
                "    - target: \$.input.path\n" +
                "      source: \$.context.path\n" +
                "  outputMappings:\n" +
                "    - target: \$.context.path\n" +
                "      source: \$.output.path\n" +
                "  choices:\n" +
                "    - condition: \$.input.[?(@.status == \"succeed\")]\n" +
                "      tasks: \n" +
                "        - category: function\n" +
                "          resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "          pattern: task_scheduler\n" +
                "          name: B1\n" +
                "          inputMappings:\n" +
                "             - target: \$.input.path\n" +
                "               source: \$.context.path\n" +
                "          outputMappings:\n" +
                "             - target: \$.context.path\n" +
                "               source: \$.output.path\n" +
                "    - condition: \$.input.[?(@.status == \"failed\")]\n" +
                "      tasks: \n" +
                "        - category: function\n" +
                "          resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "          pattern: task_scheduler\n" +
                "          name: B2\n" +
                "          inputMappings:\n" +
                "             - target: \$.input.path\n" +
                "               source: \$.context.path\n" +
                "          outputMappings:\n" +
                "             - target: \$.context.path\n" +
                "               source: \$.output.path\n" +
                "- category: function\n" +
                "  name: C\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.path\n" +
                "       source: \$.context.path\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.result\n" +
                "       source: \$.output.result\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('xxx1', dag, ["url": "http://test.com/test"])

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['status': 'succeed',
                                                        'path'  : '/donghai/ttt'],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['path': '/donghai/new/ttt'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B1")
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
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/test', 'path': '/donghai/new/ttt', 'status': 'succeed', 'result': 'http://test.com/result']
        })
        4 * callback.onEvent({ Event event -> event.eventCode == DAGEvent.TASK_FINISH.getCode() })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'A' && it.input == ['url': 'http://test.com/test'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B1' && it.input == ['path': '/donghai/ttt'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'C' && it.input == ['path': '/donghai/new/ttt'] })

    }


    /**
     * A -> B -> C
     * B为choice:
     * condition1: B1  skipped
     * condition2: B2  skipped
     *
     * @return
     */
    def "test one ChoiceTask with two FunctionTask skip dag should work well"() {
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
                "     - target: \$.context.status\n" +
                "       source: \$.output.status\n" +
                "     - target: \$.context.path\n" +
                "       source: \$.output.path\n" +
                "  next: B\n" +
                "- category: choice\n" +
                "  name: B\n" +
                "  next: C\n" +
                "  inputMappings:\n" +
                "    - target: \$.input.status\n" +
                "      source: \$.context.status\n" +
                "    - target: \$.input.path\n" +
                "      source: \$.context.path\n" +
                "  outputMappings:\n" +
                "    - target: \$.context.path\n" +
                "      source: \$.output.path\n" +
                "  choices:\n" +
                "    - condition: \$.input.[?(@.status == \"succeed\")]\n" +
                "      tasks: \n" +
                "        - category: function\n" +
                "          resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "          pattern: task_scheduler\n" +
                "          name: B1\n" +
                "          inputMappings:\n" +
                "             - target: \$.input.path\n" +
                "               source: \$.context.path\n" +
                "          outputMappings:\n" +
                "             - target: \$.context.path\n" +
                "               source: \$.output.path\n" +
                "    - condition: \$.input.[?(@.status == \"failed\")]\n" +
                "      tasks: \n" +
                "        - category: function\n" +
                "          resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "          pattern: task_scheduler\n" +
                "          name: B2\n" +
                "          inputMappings:\n" +
                "             - target: \$.input.path\n" +
                "               source: \$.context.path\n" +
                "          outputMappings:\n" +
                "             - target: \$.context.path\n" +
                "               source: \$.output.path\n" +
                "- category: function\n" +
                "  name: C\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.path\n" +
                "       source: \$.context.path\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.result\n" +
                "       source: \$.output.result\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('xxx1', dag, ["url": "http://test.com/test"])

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['status': 'skip',
                                                        'path'  : '/donghai/ttt'],
                NotifyInfo.builder()
                        .taskInfoName("A")
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
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/test', 'result': 'http://test.com/result', 'path': '/donghai/ttt', 'status': 'skip']
        })
        3 * callback.onEvent({ Event event -> event.eventCode == DAGEvent.TASK_FINISH.getCode() })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'A' && it.input == ['url': 'http://test.com/test'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'C' && it.input == ['path': '/donghai/ttt'] })
    }
}