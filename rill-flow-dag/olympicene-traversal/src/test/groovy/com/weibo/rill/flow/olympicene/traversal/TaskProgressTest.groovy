package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
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
import com.weibo.rill.flow.olympicene.traversal.checker.TimeChecker
import spock.lang.Specification

class TaskProgressTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    TimeChecker timeChecker = Mock(DefaultTimeChecker.class)
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, timeChecker, switcherManager)

    def "degrade current task only test"() {
        given:
        String text = "version: 0.0.1\n" +
                "type: flow\n" +
                "workspace: olympicene\n" +
                "dagName: test\n" +
                "tasks:\n" +
                "- category: return\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.length\n" +
                "       source: \$.context.length\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"aaa\")]\n" +
                "  progress:\n" +
                "    weight: 10\n" +
                "    args: \n" +
                "      - source: \$.input.length\n" +
                "        variable: length\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionId", dag, ['length': 10])

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.TASK_SKIPPED.getCode() &&
                        ((DAGCallbackInfo) event.data).taskInfo.getTask().getProgress().getWeight() == 10 &&
                        ((DAGCallbackInfo) event.data).taskInfo.getTaskInvokeMsg().getProgressArgs().get("length") == 10
        })
    }
}
