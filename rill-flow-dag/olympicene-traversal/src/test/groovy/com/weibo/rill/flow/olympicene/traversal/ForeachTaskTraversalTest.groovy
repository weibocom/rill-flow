package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.ForeachTaskValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import spock.lang.Specification

class ForeachTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator(), new ForeachTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    /**
     * A -> B -> C
     * B为foreach:
     * B1 -> B2
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
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B2")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

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
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/result', 'segments': ['gopUrl1', 'gopUrl2'], 'gopUrls': ['gopResultUrl2', 'gopResultUrl1']]
        })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'A' && it.input == ['url': 'http://test.com/test'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B1' && it.input == ['segmentUrl': 'gopUrl1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B2' && it.input == ['gopPath': 'gopPath1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B1' && it.input == ['segmentUrl': 'gopUrl2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B2' && it.input == ['gopPath': 'gopPath2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'C' && it.input == ['gopUrls': ['gopResultUrl2', 'gopResultUrl1']] })
    }


    /**
     * A -> B -> C
     * B为foreach:
     * B1
     *
     * @return
     */
    def "test one ForeachTask with one FunctionTask dag should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
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
                "       inputMappings:\n" +
                "          - target: \$.input.segmentUrl\n" +
                "            source: \$.context.segmentUrl\n" +
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

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl1'],
                NotifyInfo.builder()
                        .taskInfoName("B_0-B1")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl2'],
                NotifyInfo.builder()
                        .taskInfoName("B_1-B1")
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
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/result', 'segments': ['gopUrl1', 'gopUrl2'], 'gopUrls': ['gopResultUrl2', 'gopResultUrl1']]
        })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'A' && it.input == ['url': 'http://test.com/test'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_0-B1' && it.input == ['segmentUrl': 'gopUrl1'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'B_1-B1' && it.input == ['segmentUrl': 'gopUrl2'] })
        1 * dispatcher.dispatch({ it -> it.taskInfo.name == 'C' && it.input == ['gopUrls': ['gopResultUrl2', 'gopResultUrl1']] })
    }

    def "foreach task group max concurrent test"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: foreach\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "    - target: \$.input.segments\n" +
                "      source: \$.context.segments\n" +
                "  synchronization:\n" +
                "    conditions:\n" +
                "      - \$.input[?(@.segments)]\n" +
                "    maxConcurrency: 1\n" +
                "  iterationMapping:\n" +
                "    collection: \$.input.segments\n" +
                "    item: segmentUrl\n" +
                "  tasks:\n" +
                "    - category: function\n" +
                "      resourceName: \"testBusinessId::testFeatureName::testResource::prod\" \n" +
                "      pattern: task_scheduler\n" +
                "      name: A1\n" +
                "      inputMappings:\n" +
                "         - target: \$.input.segmentUrl\n" +
                "           source: \$.context.segmentUrl\n" +
                "      outputMappings:\n" +
                "         - target: \$.context.gopUrl\n" +
                "           source: \$.output.gopUrl\n"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit('xxx1', dag, ['segments': ['gopUrl1', 'gopUrl2']])

        Thread.sleep(100)
        TaskInfo A = dagStorage.getTaskInfo('xxx1', 'A')
        TaskInfo ACopy1 = DAGTraversalSerializer.deserialize(DAGTraversalSerializer.serializeToString(A).getBytes(), TaskInfo.class)

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl1'],
                NotifyInfo.builder()
                        .taskInfoName("A_0-A1")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        Thread.sleep(100)
        A = dagStorage.getTaskInfo('xxx1', 'A')
        TaskInfo ACopy2 = DAGTraversalSerializer.deserialize(DAGTraversalSerializer.serializeToString(A).getBytes(), TaskInfo.class)

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['gopUrl': 'gopResultUrl2'],
                NotifyInfo.builder()
                        .taskInfoName("A_1-A1")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        ACopy1.getSubGroupIndexToStatus().get('0') == TaskStatus.RUNNING
        ACopy1.getSubGroupIndexToStatus().get('1') == TaskStatus.READY
        ACopy1.getChildren().get('A_0-A1').getTaskStatus() == TaskStatus.RUNNING
        ACopy1.getChildren().get('A_1-A1').getTaskStatus() == TaskStatus.NOT_STARTED
        ACopy2.getSubGroupIndexToStatus().get('0') == TaskStatus.SUCCEED
        ACopy2.getSubGroupIndexToStatus().get('1') == TaskStatus.RUNNING
        ACopy2.getChildren().get('A_0-A1').getTaskStatus() == TaskStatus.SUCCEED
        ACopy2.getChildren().get('A_1-A1').getTaskStatus() == TaskStatus.RUNNING
        1 * callback.onEvent({
            Event event ->
                TaskInfo taskA = ((DAGCallbackInfo) event.data)?.dagInfo?.getTask('A')
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        taskA.getSubGroupIndexToStatus().get('0') == TaskStatus.SUCCEED &&
                        taskA.getSubGroupIndexToStatus().get('1') == TaskStatus.SUCCEED &&
                        taskA.getChildren().get('A_0-A1').getTaskStatus() == TaskStatus.SUCCEED &&
                        taskA.getChildren().get('A_1-A1').getTaskStatus() == TaskStatus.SUCCEED
        })
    }
}