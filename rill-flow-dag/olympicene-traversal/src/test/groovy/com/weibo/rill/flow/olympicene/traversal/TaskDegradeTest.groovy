package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
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
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import spock.lang.Specification


class TaskDegradeTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "degrade current task only test"() {
        given:
        String text = "version: 0.0.1\n" +
                "type: flow\n" +
                "workspace: olympicenc\n" +
                "dagName: test\n" +
                "tasks:\n" +
                "- category: pass\n" +
                "  name: A\n" +
                "  degrade:\n" +
                "    current: true\n" +
                "  next: B,C\n" +
                "- category: pass\n" +
                "  name: B\n" +
                "  next: D\n" +
                "- category: pass\n" +
                "  name: C\n" +
                "  next: E\n" +
                "- category: pass\n" +
                "  name: D\n" +
                "- category: pass\n" +
                "  name: E"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit("executionId", dag, null)

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('A')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('B')?.taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('C')?.taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('D')?.taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('E')?.taskStatus == TaskStatus.SUCCEED
        })
    }

    def "degrade following strong dependency tasks test"() {
        given:
        String text = "version: 0.0.1\n" +
                "type: flow\n" +
                "workspace: olympicenc\n" +
                "dagName: test\n" +
                "tasks:\n" +
                "- category: pass\n" +
                "  name: A\n" +
                "  degrade:\n" +
                "    followings: true\n" +
                "  next: B,C\n" +
                "- category: pass\n" +
                "  name: B\n" +
                "  next: D\n" +
                "- category: pass\n" +
                "  name: C\n" +
                "  next: E\n" +
                "- category: pass\n" +
                "  name: D\n" +
                "- category: pass\n" +
                "  name: E"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit("executionId", dag, null)

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('A')?.taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('B')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('C')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('D')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('E')?.taskStatus == TaskStatus.SKIPPED
        })
    }

    def "degrade current and following strong dependency tasks test"() {
        given:
        String text = "version: 0.0.1\n" +
                "type: flow\n" +
                "workspace: olympicenc\n" +
                "dagName: test\n" +
                "tasks:\n" +
                "- category: pass\n" +
                "  name: A\n" +
                "  degrade:\n" +
                "    current: true\n" +
                "    followings: true\n" +
                "  next: B,C\n" +
                "- category: pass\n" +
                "  name: B\n" +
                "  next: D\n" +
                "- category: pass\n" +
                "  name: C\n" +
                "  next: E\n" +
                "- category: pass\n" +
                "  name: D\n" +
                "- category: pass\n" +
                "  name: E"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit("executionId", dag, null)

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('A')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('B')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('C')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('D')?.taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo) event.data).dagInfo.getTask('E')?.taskStatus == TaskStatus.SKIPPED
        })
    }
}
