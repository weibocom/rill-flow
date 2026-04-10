package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg
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
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import spock.lang.Specification

class SuspenseTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator(), new ForeachTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "test suspense task"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "     - target: \$.context.text\n" +
                "       source: \$.output.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: B\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit('xxx1', dag, ["url": "http://test.com/test"])
        olympicene.wakeup('xxx1', ["text": "aaa"], NotifyInfo.builder().taskInfoName('A').build())
        olympicene.wakeup('xxx1', ["url": "bbb"], NotifyInfo.builder().taskInfoName('A').build())

        olympicene.finish('xxx1', DAGSettings.DEFAULT, ['url': 'http://test.com/result'],
                NotifyInfo.builder()
                        .taskInfoName("B")
                        .taskStatus(TaskStatus.SUCCEED)
                        .build())

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).executionId == 'xxx1' &&
                        ((DAGCallbackInfo) event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).context == ['url': 'http://test.com/result', 'text': 'aaa']
        })

    }

    def "suspense task status should be running if conditions unmeet"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "     - target: \$.context.text\n" +
                "       source: \$.output.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: B\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit('xxx1', dag, context)
        TaskInfo taskInfo = dagStorage.getBasicTaskInfo('xxx1', 'A')

        then:
        taskInfo.getTaskStatus() == status

        where:
        context                       | status
        [:]                           | TaskStatus.RUNNING
        ['text': 'tmp', 'url': 'bbb'] | TaskStatus.RUNNING
        ['text': 'aaa', 'url': 'bbb'] | TaskStatus.SUCCEED
    }

    def "suspense task interrupt run case"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  interruptions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit('xxx1', dag, data)

        then:
        1 * callback.onEvent({
            Event event -> event.eventCode == ret.getCode()
        })

        where:
        data            | ret
        ["url": "bbb"]  | DAGEvent.TASK_FINISH
        ["text": "aaa"] | DAGEvent.TASK_FAILED
    }

    def "suspense task interrupt wakeup case"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "     - target: \$.context.text\n" +
                "       source: \$.output.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  interruptions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n"
        DAG dag = dagParser.parse(text)
        

        when:
        olympicene.submit('xxx1', dag, [:])
        olympicene.wakeup('xxx1', data, NotifyInfo.builder().taskInfoName('A').build())

        then:
        1 * callback.onEvent({ Event event -> event.eventCode == ret.getCode()})

        where:
        data            | ret
        ["url": "bbb"]  | DAGEvent.TASK_FINISH
        ["text": "aaa"] | DAGEvent.TASK_FAILED
    }
    def "suspense task should be SKIPPED on timeout when skipOnTimeout=true and tolerance=true"() {
        given:
        // skipOnTimeout=true 且 tolerance=true，超时后节点应变为 SKIPPED
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  tolerance: true\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: \"120\"\n" +
                "    skipOnTimeout: \"true\"\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: B\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)

        when:
        // 提交 DAG，suspense 节点进入 RUNNING 状态等待唤醒
        olympicene.submit('timeout_skip_1', dag, [:])
        // 模拟超时：TimeCheckRunner 会以 taskStatus=FAILED, msg="timeout" 调用 finishTaskSync
        olympicene.wakeup('timeout_skip_1', [:],
                NotifyInfo.builder()
                        .taskInfoName('A')
                        .taskStatus(TaskStatus.FAILED)
                        .taskInvokeMsg(TaskInvokeMsg.builder().msg("timeout").build())
                        .build())
        TaskInfo taskInfo = dagStorage.getBasicTaskInfo('timeout_skip_1', 'A')

        then:
        // skipOnTimeout=true && tolerance=true && timeout → 应为 SKIPPED
        taskInfo.getTaskStatus() == TaskStatus.SKIPPED
    }

    def "suspense task should be FAILED on timeout when skipOnTimeout=false"() {
        given:
        // skipOnTimeout=false，超时后节点应触发 TASK_FAILED 事件（不被跳过）
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  tolerance: true\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: \"120\"\n" +
                "    skipOnTimeout: \"false\"\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('timeout_noskip_1', dag, [:])
        olympicene.wakeup('timeout_noskip_1', [:],
                NotifyInfo.builder()
                        .taskInfoName('A')
                        .taskStatus(TaskStatus.FAILED)
                        .taskInvokeMsg(TaskInvokeMsg.builder().msg("timeout").build())
                        .build())

        then:
        // skipOnTimeout=false → 触发 TASK_FAILED 回调事件，而不是 TASK_SKIPPED
        1 * callback.onEvent({
            Event event -> event.eventCode == DAGEvent.TASK_FAILED.getCode()
        })
    }

    def "suspense task should be FAILED on timeout when tolerance=false even if skipOnTimeout=true"() {
        given:
        // tolerance=false，即使 skipOnTimeout=true，超时后也应触发 TASK_FAILED 事件
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  tolerance: false\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: \"120\"\n" +
                "    skipOnTimeout: \"true\"\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('timeout_tol_false_1', dag, [:])
        olympicene.wakeup('timeout_tol_false_1', [:],
                NotifyInfo.builder()
                        .taskInfoName('A')
                        .taskStatus(TaskStatus.FAILED)
                        .taskInvokeMsg(TaskInvokeMsg.builder().msg("timeout").build())
                        .build())

        then:
        // tolerance=false → 触发 TASK_FAILED 回调事件，而不是 TASK_SKIPPED
        1 * callback.onEvent({
            Event event -> event.eventCode == DAGEvent.TASK_FAILED.getCode()
        })
    }
    def "suspense task should remain FAILED on interruption even if skipOnTimeout=true and tolerance=true"() {
        given:
        // tolerance=true 且 skipOnTimeout=true，但触发的是打断而非超时，应保持 FAILED
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  tolerance: true\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: \"120\"\n" +
                "    skipOnTimeout: \"true\"\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  interruptions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n"
        DAG dag = dagParser.parse(text)

        when:
        // 提交 DAG，初始上下文触发 interruption 条件（text=aaa）
        olympicene.submit('interrupt_with_skip_flag_1', dag, ["text": "aaa"])

        then:
        // 虽然 tolerance=true && skipOnTimeout=true，但触发的是打断而非超时，应为 TASK_FAILED 而非 TASK_SKIPPED
        1 * callback.onEvent({
            Event event -> event.eventCode == DAGEvent.TASK_FAILED.getCode()
        })
    }
}