package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGResult
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.result.DAGResultHandler
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
import com.weibo.rill.flow.olympicene.traversal.result.LocalSyncDAGResultHandler
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class FlowSyncTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    DAGResultHandler dagResultHandler = new LocalSyncDAGResultHandler()
    ExecutorService executor = Executors.newFixedThreadPool(10)
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, dagStorageProcedure, null, dagResultHandler, Mock(DAGDispatcher.class), Mock(DefaultTimeChecker.class), executor, switcherManager)

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
                "  next: \n" +
                "- category: pass\n" +
                "  name: B\n" +
                "  next: "
                DAG dag = dagParser.parse(text)

        when:
        DAGResult dagResult = olympicene.run('xxx2', dag, ["key1": "value1", "key2": "value2"])

        then:
        dagResult.getContext() == ["key1": "value1", "key2": "value2"]
        dagResult.getDagInfo().getTask("A").getTaskStatus() == TaskStatus.SUCCEED &&
                dagResult.getDagInfo().getTask("B").getTaskStatus() == TaskStatus.SUCCEED
    }
}