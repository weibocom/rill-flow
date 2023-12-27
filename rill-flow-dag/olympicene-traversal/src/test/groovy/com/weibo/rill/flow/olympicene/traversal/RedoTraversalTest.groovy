package com.weibo.rill.flow.olympicene.traversal

import com.google.common.collect.Lists
import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
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
import groovy.util.logging.Slf4j
import spock.lang.Specification


@Slf4j
class RedoTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator(), new ForeachTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)
    String executionId = "xxx1"
    DAG dag

    def setup() {
        String text = "version: 0.0.1\n" +
                "workspace: olympicene\n" +
                "dagName: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "  next: B\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: \"olympicene::test::funtion1::prod\" \n" +
                "  pattern: task_scheduler"
        dag = dagParser.parse(text)
    }

    def "redo unsuccess task"() {
        given:
     

        when:
        olympicene.submit(executionId, dag, ["url": "http://test.com/test"])
        olympicene.wakeup(executionId, ["url": "bbb"], NotifyInfo.builder().taskInfoName('A').build())

        olympicene.redo(executionId, ["url": "http://test.com/test"], null)
        DAGInfo dagInfo = dagStorage.getDAGInfo(executionId)

        then:
        dagInfo.getTask("A").getTaskStatus() == TaskStatus.SUCCEED
        dagInfo.getTask("B").getTaskStatus() == TaskStatus.RUNNING
        2 * dispatcher.dispatch(*_)
    }

    def "redo assigned task"() {
        given:
     

        when:
        olympicene.submit(executionId, dag, ["url": "http://test.com/test"])
        olympicene.wakeup(executionId, ["url": "bbb"], NotifyInfo.builder().taskInfoName('A').build())

        List<String> taskNames = Lists.newArrayList("A", "A_", "A1")
        olympicene.redo(executionId, ["url": "http://test.com/test"], NotifyInfo.builder().taskInfoNames(taskNames).build())
        DAGInfo dagInfo = dagStorage.getDAGInfo(executionId)

        then:
        dagInfo.getTask("A").getTaskStatus() == TaskStatus.RUNNING
        dagInfo.getTask("B").getTaskStatus() == TaskStatus.NOT_STARTED
    }
}
