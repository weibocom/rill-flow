package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.model.DAGSettings
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
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
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
class MultiDAGTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator(), new ForeachTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager)
    DAG dag

    def setup() {
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: rillflow://descriptorId \n" +
                "  pattern: flow_sync\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "     - target: \$.input.text\n" +
                "       source: \$.context.text\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "     - target: \$.context.text\n" +
                "       source: \$.output.text\n"
        dag = dagParser.parse(text)
    }

    def "task record sub dag executionId and dag record execute route"() {
        given:
        dispatcher.dispatch(*_) >> "{\"execution_id\":\"level2\"}"
        when:
        olympicene.submit("level1", dag, [:])
        NotifyInfo notifyInfo = NotifyInfo.builder()
                .parentDAGExecutionId("level1")
                .parentDAGTaskInfoName("A").build()
        olympicene.submit("level2", dag, [:], DAGSettings.DEFAULT, notifyInfo)
        TaskInfo level1TaskA = dagStorage.getDAGInfo("level1").getTask("A")
        DAGInfo level2 = dagStorage.getDAGInfo("level2")

        then:
        level1TaskA.getTaskInvokeMsg().getReferencedDAGExecutionId() == "level2"
        level2.getDagInvokeMsg().getExecutionRoutes().get(0).getExecutionId() == "level1"
        level2.getDagInvokeMsg().getExecutionRoutes().get(0).getTaskInfoName() == "A"
    }

    def "depth should not exceed setting value"() {
        given:
        dispatcher.dispatch(*_) >> "{\"execution_id\":\"xxx\"}"
        when:
        DAGSettings dagSettings = DAGSettings.builder().dagMaxDepth(2).build()

        olympicene.submit("level1", dag, [:])

        NotifyInfo notifyInfoLevel2 = NotifyInfo.builder()
                .parentDAGExecutionId("level1")
                .parentDAGTaskInfoName("A").build()
        olympicene.submit("level2", dag, [:], dagSettings, notifyInfoLevel2)

        NotifyInfo notifyInfoLevel3 = NotifyInfo.builder()
                .parentDAGExecutionId("level2")
                .parentDAGTaskInfoName("A").build()
        olympicene.submit("level3", dag, [:], dagSettings, notifyInfoLevel3)

        then:
        def e = thrown(DAGTraversalException)
        e.getMessage() == "exceed max depth, dag route: level1#A->level2#A"
    }
}
