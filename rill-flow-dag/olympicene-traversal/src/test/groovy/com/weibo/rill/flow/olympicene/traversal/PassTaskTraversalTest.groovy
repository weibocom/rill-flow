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
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import spock.lang.Specification

class PassTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "test one passTask dag should work well"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: pass\n" +
                "  name: A\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.key2\n" +
                "       source: \$.context.key1\n" +
                "     - target: \$.context.key1\n" +
                "       source: 'value1'"
        DAG dag = dagParser.parse(text)
      

        when:
        olympicene.submit('xxx2', dag, ["key1": "value2"])

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() && ((DAGCallbackInfo) event.data).context == ['key1': 'value1', 'key2': 'value2']
        })
    }

    /**
     * A --> B --> C
     * B is passTask
     * @return
     */
    def "test three tasks dag should work well"() {
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
                "- category: pass\n" +
                "  name: testNameB\n" +
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
                ['url': 'http://result'],
                NotifyInfo.builder()
                        .taskInfoName("testNameC")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        2 * dispatcher.dispatch(_)
        1 * callback.onEvent({ Event event ->
            event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                    ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                    ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED
        })
    }
}