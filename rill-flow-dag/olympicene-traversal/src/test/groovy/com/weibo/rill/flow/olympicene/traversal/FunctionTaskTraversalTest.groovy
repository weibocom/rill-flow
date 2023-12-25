package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import spock.lang.Specification

class FunctionTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])

    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "test one functionTask dag should work well"() {
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
                "  progress: \n" +
                "    weight: 10\n" +
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
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('xxx2', dag, ["key1": "value1", "key2": "value2"])
        olympicene.finish('xxx2',
                DAGSettings.DEFAULT,
                ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        1 * dispatcher.dispatch(_)
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx2' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask("A").getTask().getProgress().getWeight() == 10 &&
                        ((DAGCallbackInfo) event.data).context == ['key1': 'value1', 'key2': 'value2', 'segments': ['gopUrl1', 'gopUrl2']]
        })
    }

    /**
     * A --> B --> C
     * @return
     */
    def "test three functionTasks dag should work well"() {
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
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        3 * dispatcher.dispatch(_)
        1 * callback.onEvent({ Event event ->
            event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                    ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                    ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                    ((DAGCallbackInfo) event.data).context == ['mediaUrl': 'http://xxx', 'segments': ['gopUrl1', 'gopUrl2'], 'url': 'http://result', 'path': 'sss/sss/uuu']
        })
    }


    /**
     * B --> C
     * A --> C
     * @return
     */
    def "test three functionTasks dag should work well2"() {
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
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        3 * dispatcher.dispatch(_)
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).context == ['mediaUrl': 'http://xxx', 'segments': ['gopUrl1', 'gopUrl2'], 'url': 'http://result', 'path': 'sss/sss/uuu']
        })
    }

    /**
     * A --> B
     * A --> C
     * @return
     */
    def "test three functionTasks dag should work well3"() {
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
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        3 * dispatcher.dispatch(_)
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).context == ['mediaUrl': 'http://xxx', 'segments': ['gopUrl1', 'gopUrl2'], 'url': 'http://result', 'path': 'sss/sss/uuu']
        })
    }


    def "test one functionTask failed dag should work well"() {
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
                "     - target: \$.input.key1\n" +
                "       source: \$.context.key1\n" +
                "     - target: \$.input.key2\n" +
                "       source: \$.context.key2\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: "
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('xxx2', dag, ["key1": "value1", "key2": "value2"])
        olympicene.finish('xxx2',
                DAGSettings.DEFAULT,
                ['segments': ['gopUrl1', 'gopUrl2']],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(TaskStatus.FAILED)
                        .build())

        then:
        1 * callback.onEvent({ Event event -> event.eventCode == DAGEvent.DAG_FAILED.getCode() })
        1 * callback.onEvent({ Event event -> event.eventCode == DAGEvent.TASK_FAILED.getCode() })
    }

    def "task sync function should complete when invoke dispatch"() {
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
                "  pattern: task_sync\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.key1\n" +
                "       source: \$.context.key1\n" +
                "     - target: \$.input.key2\n" +
                "       source: \$.context.key2\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: "
        DAG dag = dagParser.parse(text)
        dispatcher.dispatch(*_) >> dispatchRet
        

        when:
        olympicene.submit('xxx2', dag, ["key1": "value1", "key2": "value2"])

        then:
        1 * callback.onEvent({ Event event -> event.eventCode == taskEvent.getCode() })
        1 * callback.onEvent({ Event event -> event.eventCode == dagEvent.getCode() })

        where:
        dispatchRet                 | taskEvent            | dagEvent
        null                        | DAGEvent.TASK_FAILED | DAGEvent.DAG_FAILED
        "not json format"           | DAGEvent.TASK_FAILED | DAGEvent.DAG_FAILED
        '{}'                        | DAGEvent.TASK_FINISH | DAGEvent.DAG_SUCCEED
        '[]'                        | DAGEvent.TASK_FINISH | DAGEvent.DAG_SUCCEED
        '{"result_type":"aaa"}'     | DAGEvent.TASK_FAILED | DAGEvent.DAG_FAILED
        '{"result_type":"success"}' | DAGEvent.TASK_FINISH | DAGEvent.DAG_SUCCEED
    }

    def "task sync function status condition check"() {
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
                "  pattern: task_sync\n" +
                "  inputMappings:\n" +
                "    - target: \$.input.key1\n" +
                "      source: \$.context.key1\n" +
                "    - target: \$.input.key2\n" +
                "      source: \$.context.key2\n" +
                "  successConditions:\n" +
                "    - \$.output[?(@.signal == true)]"
        DAG dag = dagParser.parse(text)
        dispatcher.dispatch(*_) >> dispatchRet
        

        when:
        olympicene.submit('xxx2', dag, ["key1": "value1", "key2": "value2"])

        then:
        1 * callback.onEvent({ Event event -> event.eventCode == taskEvent.getCode() })

        where:
        dispatchRet        | taskEvent
        null               | DAGEvent.TASK_FAILED
        "not json format"  | DAGEvent.TASK_FAILED
        '{}'               | DAGEvent.TASK_FAILED
        '[]'               | DAGEvent.TASK_FAILED
        '{"signal":false}' | DAGEvent.TASK_FAILED
        '{"signal":true}'  | DAGEvent.TASK_FINISH
    }

    def "task async function status condition check"() {
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
                "    - target: \$.input.key1\n" +
                "      source: \$.context.key1\n" +
                "    - target: \$.input.key2\n" +
                "      source: \$.context.key2\n" +
                "  successConditions:\n" +
                "    - \$.output[?(@.signal == true)]"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit('xxx1', dag, ["key1": "value1", "key2": "value2"])
        olympicene.finish('xxx1',
                DAGSettings.DEFAULT,
                ['signal': signal],
                NotifyInfo.builder()
                        .taskInfoName("A")
                        .taskStatus(taskStatus)
                        .build())

        then:
        1 * callback.onEvent({ Event event -> event.eventCode == taskEvent.getCode() })

        where:
        signal | taskStatus         | taskEvent
        _      | TaskStatus.SUCCEED | DAGEvent.TASK_FAILED
        _      | TaskStatus.SUCCEED | DAGEvent.TASK_FAILED
        _      | TaskStatus.FAILED  | DAGEvent.TASK_FAILED
        _      | TaskStatus.FAILED  | DAGEvent.TASK_FAILED
        null   | null               | DAGEvent.TASK_FAILED
        "true" | null               | DAGEvent.TASK_FAILED
        true   | null               | DAGEvent.TASK_FINISH
        false  | null               | DAGEvent.TASK_FAILED
    }
}