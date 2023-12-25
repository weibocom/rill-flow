package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.interfaces.model.task.FunctionPattern
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import spock.lang.Specification

class InvokeMsgTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator()])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "big flow taskInfo and small flow dagInfo and small flow taskInfo invokeMsg"() {
        given:
        String bigFlowYaml = "version: 0.0.1\n" +
                "workspace: olympicene\n" +
                "dagName: bigFlow\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: flow://olympicene:smallFlow\n" +
                "  pattern: flow_sync"
        String smallFlowYaml = "version: 0.0.1\n" +
                "workspace: olympicene\n" +
                "dagName: smallFlow\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "  - category: foreach\n" +
                "    name: B\n" +
                "    inputMappings:\n" +
                "      - target: \$.input.segments\n" +
                "        source: \$.context.segments\n" +
                "    iterationMapping:\n" +
                "      collection: \$.input.segments\n" +
                "      item: segmentUrl\n" +
                "    tasks:\n" +
                "      - category: function\n" +
                "        name: C\n" +
                "        resourceName: function://testBusinessId\n" +
                "        pattern: task_scheduler\n" +
                "defaultContext:\n" +
                "  segments: '[\"gopUrl\"]'"

        DAG bigFlow = dagParser.parse(bigFlowYaml)
        DAG smallFlow = dagParser.parse(smallFlowYaml)
        dispatcher.dispatch(*_) >> '{"execution_id":"smallFlow"}'
      

        when:
        olympicene.submit("bigFlow", bigFlow, [:])

        NotifyInfo smallFlowSubmit = NotifyInfo.builder()
                .parentDAGExecutionId("bigFlow")
                .parentDAGTaskInfoName("A")
                .parentDAGTaskExecutionType(FunctionPattern.FLOW_SYNC)
                .build()
        olympicene.submit("smallFlow", smallFlow, [:], DAGSettings.DEFAULT, smallFlowSubmit)

        NotifyInfo smallFlowNotify = NotifyInfo.builder()
                .taskInfoName("B_0-C")
                .taskStatus(TaskStatus.FAILED)
                .taskInvokeMsg(TaskInvokeMsg.builder().code("code").msg("msg").ext(['ext':'info']).build())
                .build()
        olympicene.finish("smallFlow", DAGSettings.DEFAULT, [:], smallFlowNotify)

        then:
        1 * callback.onEvent({Event event ->
            event.eventCode == DAGEvent.TASK_FAILED.getCode() &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getName() == "B_0-C" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getCode() == "code" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getMsg() == "msg" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getExt() == ['ext':'info']
        })
        1 * callback.onEvent({Event event ->
            event.eventCode == DAGEvent.TASK_FAILED.getCode() &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getName() == "B" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getCode() == "code" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getMsg() == "msg" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getExt() == ['ext':'info']
        })
        1 * callback.onEvent({Event event ->
            event.eventCode == DAGEvent.DAG_FAILED.getCode() &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getExecutionId() == "smallFlow" &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getCode() == "code" &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getMsg() == "msg" &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getExt() == ['ext':'info']
        })
        1 * callback.onEvent({Event event ->
            event.eventCode == DAGEvent.TASK_FAILED.getCode() &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getName() == "A" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getCode() == "code" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getMsg() == "msg" &&
                    ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getExt() == ['ext':'info']
        })
        1 * callback.onEvent({Event event ->
            event.eventCode == DAGEvent.DAG_FAILED.getCode() &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getExecutionId() == "bigFlow" &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getCode() == "code" &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getMsg() == "msg" &&
                    ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getExt() == ['ext':'info']
        })
    }
}
