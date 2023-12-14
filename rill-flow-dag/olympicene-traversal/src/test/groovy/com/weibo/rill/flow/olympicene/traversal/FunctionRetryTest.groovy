package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import spock.lang.Specification

class FunctionRetryTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator()])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    DefaultTimeChecker timeChecker = Mock(DefaultTimeChecker.class)
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, timeChecker, switcherManager)
    DAG dag

    def setup() {
        String text = "version: 0.0.1\n" +
                "namespace: xltest\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks:\n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: testBusinessId::testFeatureName::testResource::prod\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  retry:\n" +
                "    intervalInSeconds: 3\n" +
                "    maxRetryTimes: 2\n" +
                "    multiplier: 1"
        dag = dagParser.parse(text)
    }

    def "function dispatch task fail retry test"() {
        given:
        ((FunctionTask) dag.getTasks().get(0)).getRetry().setIntervalInSeconds(interval)

        when:
        olympicene.submit('executionId', dag, [:])

        then:
        dispatchTimes * dispatcher.dispatch(*_) >> { throw new Exception("handle fails") }
        timeCheckTimes * timeChecker.addMemberToCheckPool(*_)
        dagEventTimes * callback.onEvent({
            event ->
                event.getEventCode() == DAGEvent.TASK_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.FAILED
        })
        taskEventTimes * callback.onEvent({
            event ->
                event.getEventCode() == DAGEvent.DAG_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.FAILED
        })

        where:
        interval | dispatchTimes | timeCheckTimes | dagEventTimes | taskEventTimes
        -1       | 3             | 0              | 1             | 1
        0        | 3             | 0              | 1             | 1
        1        | 1             | 1              | 0             | 0
    }

    def "function finish status fails retry test"() {
        given:
        ((FunctionTask) dag.getTasks().get(0)).getRetry().setIntervalInSeconds(interval)

        when:
        olympicene.submit('executionId', dag, [:])
        olympicene.finish('executionId', DAGSettings.DEFAULT, [:],
                NotifyInfo.builder().taskInfoName("A").taskStatus(TaskStatus.FAILED).build())

        then:
        dispatchTimes * dispatcher.dispatch(*_)
        timeCheckTimes * timeChecker.addMemberToCheckPool(*_)
        0 * callback.onEvent(*_)

        where:
        interval | dispatchTimes | timeCheckTimes
        -1       | 2             | 0
        0        | 2             | 0
        1        | 1             | 1
    }

}
