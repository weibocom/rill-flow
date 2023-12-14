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
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import spock.lang.Specification

class FunctionTaskToleranceTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "test one functionTask skip dag should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  tolerance: true\n" +
                "  resourceName: \"olympicene::test::function1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.key1\n" +
                "       source: \$.context.key1\n" +
                "     - target: \$.input.key2\n" +
                "       source: \$.context.key2\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: "
      

        when:
        DAG dag = dagParser.parse(text)
        olympicene.submit('xxx2', dag, ["key1": "value1", "key2": "value2"])
        olympicene.finish('xxx2',
                DAGSettings.DEFAULT,
                ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(TaskStatus.FAILED)
                        .build())

        then:
        dag.tasks.get(0).tolerance
        1 * dispatcher.dispatch(_)
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx2' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('A').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).context == ['key1': 'value1', 'key2': 'value2', 'segments': ['gopUrl1', 'gopUrl2']]
        })
    }

    /**
     * A --> B --> C
     */
    def "test three functionTasks dag, B is Skip should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: testNameA\n" +
                "  resourceName: \"olympicene::test::funtionA::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.mediaUrl\n" +
                "       source: \$.context.mediaUrl\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: testNameB\n" +
                "- category: function\n" +
                "  name: testNameB\n" +
                "  tolerance: true\n" +
                "  resourceName: \"olympicene::test::funtionB::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.segments\n" +
                "       source: \$.context.segments\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.path\n" +
                "       source: \$.output.path\n" +
                "  next: testNameC\n" +
                "- category: function\n" +
                "  name: testNameC\n" +
                "  resourceName: \"olympicene::test::funtionC::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.path\n" +
                "       source: \$.context.path\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n"
      

        when:
        DAG dag = dagParser.parse(text)
        olympicene.submit('xxx1', dag, ["mediaUrl": "http://xxx"])

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("testNameA")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['path': 'sss/sss/uuu'],
                NotifyInfo.builder()
                        .taskInfoName("testNameB")
                        .taskStatus(TaskStatus.FAILED)
                        .build())

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['url': 'http://result'],
                NotifyInfo.builder()
                        .taskInfoName("testNameC")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        3 * dispatcher.dispatch(_)
        1 * callback.onEvent({ Event event ->
            event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                    ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                    ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                    ((DAGCallbackInfo) event.data).dagInfo.getTask('testNameB').taskStatus == TaskStatus.SKIPPED &&
                    ((DAGCallbackInfo) event.data).context == ['mediaUrl': 'http://xxx', 'segments': ['gopUrl1', 'gopUrl2'], 'path': 'sss/sss/uuu', 'url': 'http://result']
        })
    }


    /**
     * B --> C
     * A --> C
     */
    def "test three functionTasks dag, C is skip should work well2"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: testNameA\n" +
                "  resourceName: \"olympicene::test::funtionA::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.mediaUrl\n" +
                "       source: \$.context.mediaUrl\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: testNameC\n" +
                "- category: function\n" +
                "  name: testNameB\n" +
                "  resourceName: \"olympicene::test::funtionB::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.mediaUrl\n" +
                "       source: \$.context.mediaUrl\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.path\n" +
                "       source: \$.output.path\n" +
                "  next: testNameC\n" +
                "- category: function\n" +
                "  name: testNameC\n" +
                "  tolerance: true\n" +
                "  resourceName: \"olympicene::test::functionC::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.path\n" +
                "       source: \$.context.path\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "  next: "
        DAG dag = dagParser.parse(text)
      

        when:
        olympicene.submit('xxx1', dag, ["mediaUrl": "http://xxx"])

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("testNameA")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['path': 'sss/sss/uuu'],
                NotifyInfo.builder()
                        .taskInfoName("testNameB")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['url': 'http://result'],
                NotifyInfo.builder()
                        .taskInfoName("testNameC")
                        .taskStatus(TaskStatus.FAILED)
                        .build())

        then:
        3 * dispatcher.dispatch(_)
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('testNameC').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).context == ['mediaUrl': 'http://xxx', 'segments': ['gopUrl1', 'gopUrl2'], 'path': 'sss/sss/uuu', 'url': 'http://result']
        })
    }

    /**
     * A --> B
     * A --> C
     */
    def "test three functionTasks dag, A is skip should work well3"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: testNameA\n" +
                "  tolerance: true\n" +
                "  resourceName: \"olympicene::test::funtionA::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.mediaUrl\n" +
                "       source: \$.context.mediaUrl\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: testNameB,testNameC\n" +
                "- category: function\n" +
                "  name: testNameB\n" +
                "  resourceName: \"olympicene::test::funtionB::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.mediaUrl\n" +
                "       source: \$.context.mediaUrl\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.path\n" +
                "       source: \$.output.path\n" +
                "  next: \n" +
                "- category: function\n" +
                "  name: testNameC\n" +
                "  resourceName: \"olympicene::test::funtionC::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.path\n" +
                "       source: \$.context.path\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "  next: "
        DAG dag = dagParser.parse(text)
      

        when:
        olympicene.submit('xxx1', dag, ["mediaUrl": "http://xxx"])

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("testNameA")
                        .taskStatus(TaskStatus.FAILED)
                        .build())

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['path': 'sss/sss/uuu'],
                NotifyInfo.builder()
                        .taskInfoName("testNameB")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['url': 'http://result'],
                NotifyInfo.builder()
                        .taskInfoName("testNameC")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        3 * dispatcher.dispatch(_)
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('testNameA').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).context == ['mediaUrl': 'http://xxx', 'segments': ['gopUrl1', 'gopUrl2'], 'path': 'sss/sss/uuu', 'url': 'http://result']
        })
    }

    /**
     * A -> B -> C
     * B为foreach:
     * B1 -> B2
     *
     * B1 tolerence == true
     *
     * @return
     */
    def "test one ForeachTask with two FunctionTask dag should work well"() {
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
                "  next: B\n" +
                "- category: foreach\n" +
                "  name: B\n" +
                "  inputMappings:\n" +
                "    - target: \$.input.segments\n" +
                "      source: \$.context.segments\n" +
                "  iterationMapping:\n" +
                "      collection: \$.input.segments\n" +
                "      item: segmentUrl\n" +
                "  outputMappings:\n" +
                "    - target: \$.context.gopUrls\n" +
                "      source: \$.output.sub_context.[*].gopUrl\n" +
                "  next: C\n" +
                "  tasks:\n" +
                "     - category: function\n" +
                "       resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "       pattern: task_scheduler\n" +
                "       name: B1\n" +
                "       tolerance: true\n" +
                "       next: B2\n" +
                "       inputMappings:\n" +
                "          - target: \$.input.segmentUrl\n" +
                "            source: \$.context.segmentUrl\n" +
                "       outputMappings:\n" +
                "          - target: \$.context.gopPath\n" +
                "            source: \$.output.gopPath\n" +
                "     - category: function\n" +
                "       resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "       name: B2\n" +
                "       pattern: task_scheduler\n" +
                "       inputMappings:\n" +
                "          - target: \$.input.gopPath\n" +
                "            source: \$.context.gopPath\n" +
                "       outputMappings:\n" +
                "          - target: \$.context.gopUrl\n" +
                "            source: \$.output.gopUrl\n" +
                "- category: function\n" +
                "  name: C\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n"
        DAG dag = dagParser.parse(text)
      

        when:
        olympicene.submit('xxx1', dag, ["url": "http://test.com/test"])

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopPath': 'gopPath1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B1")
                        .taskStatus(TaskStatus.FAILED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B2")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build());

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopPath': 'gopPath2'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B1")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl2'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B2")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['url': 'http://test.com/result'],
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
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('B').getChildren().get('B_0-B1').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/result', 'segments': ['gopUrl1', 'gopUrl2'], 'gopUrls': ['gopResultUrl2', 'gopResultUrl1']]
        })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'A' && it.input == ['url': 'http://test.com/test'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B1' && it.input == ['segmentUrl': 'gopUrl1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B2' && it.input == ['gopPath': 'gopPath1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B1' && it.input == ['segmentUrl': 'gopUrl2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B2' && it.input == ['gopPath': 'gopPath2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'C' && it.input == ['gopUrls': ['gopResultUrl2', 'gopResultUrl1']] })
    }
}