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
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import spock.lang.Specification


class ReturnTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator()])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)

    def "if return task status is success then next tasks status should be skip"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: return\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: B\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: \"function://olympicene::test::funtion1::prod\"\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['text':'aaa', 'url':"bbb"])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('A').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('B').taskStatus == TaskStatus.SKIPPED
        })
    }

    def "if return task is in sub task"() {
        given:
        String text = "workspace: default\n" +
                "dagName: switchSubTask\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[]'\n" +
                "tasks:\n" +
                "  - name: foreach\n" +
                "    description: ''\n" +
                "    synchronization:\n" +
                "      conditions: []\n" +
                "    iterationMapping:\n" +
                "      item: info\n" +
                "      index: index\n" +
                "      collection: \$.input.infos_array\n" +
                "    inputMappings:\n" +
                "      - transform: return seq.list(0, 1, 2);\n" +
                "        target: \$.input.infos_array\n" +
                "    category: foreach\n" +
                "    tasks:\n" +
                "      - name: A\n" +
                "        description: ''\n" +
                "        category: pass\n" +
                "        title: ''\n" +
                "      - name: B\n" +
                "        description: ''\n" +
                "        category: pass\n" +
                "        title: ''\n" +
                "      - name: returnA\n" +
                "        category: return\n" +
                "        title: ''\n" +
                "        description: ''\n" +
                "        inputMappings:\n" +
                "          - source: \$.context.index\n" +
                "            target: \$.input.index\n" +
                "        conditions:\n" +
                "          - \$.input.[?(@.index == 1)]\n" +
                "        next: A\n" +
                "      - name: returnB\n" +
                "        category: return\n" +
                "        title: ''\n" +
                "        description: ''\n" +
                "        inputMappings:\n" +
                "          - source: \$.context.index\n" +
                "            target: \$.input.index\n" +
                "        conditions:\n" +
                "          - \$.input.[?(@.index == 0)]\n" +
                "        next: B\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':15])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_FINISH.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_0-A")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SUCCEED
            }
        })
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_SKIPPED.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_0-B")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SKIPPED
            }
        })
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_SKIPPED.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_1-A")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SKIPPED
            }
        })
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_FINISH.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_1-B")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SUCCEED
            }
        })
    }

    def "if return task status is skip then next tasks status should not be skip"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: return\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: B\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: \"function://olympicene::test::funtion1::prod\"\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('executionIdSkip', dag, ['text':'aaa'])
        def dagInfo = dagStorage.getDAGInfo('executionIdSkip')

        then:
        dagInfo.dagStatus == DAGStatus.RUNNING
        dagInfo.tasks.get('A').taskStatus == TaskStatus.SKIPPED
        dagInfo.tasks.get('B').taskStatus == TaskStatus.RUNNING
    }

    def "return task skip next task check"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks:\n" +
                "- category: pass\n" +
                "  name: A\n" +
                "  next: B,C\n" +
                "- category: return\n" +
                "  name: B\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: D\n" +
                "- category: function\n" +
                "  name: C\n" +
                "  resourceName: olympicene::test::function1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: E\n" +
                "- category: function\n" +
                "  name: D\n" +
                "  resourceName: olympicene::test::function1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments\n" +
                "  next: E\n" +
                "- category: function\n" +
                "  name: E\n" +
                "  resourceName: olympicene::test::function1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.segments\n" +
                "       source: \$.output.segments"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('executionIdNextCheck', dag, ['text':'aaa', 'url':"bbb"])
        def dagInfo = dagStorage.getDAGInfo('executionIdNextCheck')

        then:
        dagInfo.dagStatus == DAGStatus.RUNNING
        dagInfo.tasks.get('A').taskStatus == TaskStatus.SUCCEED
        dagInfo.tasks.get('B').taskStatus == TaskStatus.SUCCEED
        dagInfo.tasks.get('C').taskStatus == TaskStatus.RUNNING
        dagInfo.tasks.get('D').taskStatus == TaskStatus.SKIPPED
        dagInfo.tasks.get('E').taskStatus == TaskStatus.NOT_STARTED
    }

    def "task dependent multi return task check"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks:\n" +
                "- category: pass\n" +
                "  name: A\n" +
                "  next: B,C\n" +
                "- category: return\n" +
                "  name: B\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  next: D\n" +
                "- category: return\n" +
                "  name: C\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.text == \"aaa\")]\n" +
                "  next: E\n" +
                "- category: pass\n" +
                "  name: D\n" +
                "- category: pass\n" +
                "  name: E\n" +
                "  next: D"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit('executionIdNextCheck', dag, context)

        then:
        1 * callback.onEvent({
            Event event ->
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo) event.data).dagInfo.tasks.get('A').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo) event.data).dagInfo.tasks.get('B').taskStatus == taskBStatus &&
                        ((DAGCallbackInfo) event.data).dagInfo.tasks.get('C').taskStatus == taskCStatus &&
                        ((DAGCallbackInfo) event.data).dagInfo.tasks.get('D').taskStatus == taskDStatus &&
                        ((DAGCallbackInfo) event.data).dagInfo.tasks.get('E').taskStatus == taskEStatus
        })

        where:
        context                       | taskBStatus        | taskCStatus        | taskDStatus        | taskEStatus
        ['text': 'aaa', 'url': "bbb"] | TaskStatus.SUCCEED | TaskStatus.SUCCEED | TaskStatus.SKIPPED | TaskStatus.SKIPPED
        ['text': 'aa1', 'url': "bbb"] | TaskStatus.SUCCEED | TaskStatus.SKIPPED | TaskStatus.SUCCEED | TaskStatus.SUCCEED
        ['text': 'aaa', 'url': "bb1"] | TaskStatus.SKIPPED | TaskStatus.SUCCEED | TaskStatus.SUCCEED | TaskStatus.SKIPPED
        ['text': 'aa1', 'url': "bb1"] | TaskStatus.SKIPPED | TaskStatus.SKIPPED | TaskStatus.SUCCEED | TaskStatus.SUCCEED
    }
}
