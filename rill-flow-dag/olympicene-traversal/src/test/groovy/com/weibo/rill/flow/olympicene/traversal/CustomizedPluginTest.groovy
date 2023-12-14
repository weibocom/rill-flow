package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.olympicene.core.constant.SystemConfig
import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.interfaces.model.task.FunctionTask
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult
import com.weibo.rill.flow.olympicene.traversal.helper.SameThreadExecutorService
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer
import com.weibo.rill.flow.olympicene.traversal.runners.DAGRunner
import com.weibo.rill.flow.olympicene.traversal.runners.FunctionTaskRunner
import com.weibo.rill.flow.olympicene.traversal.runners.TaskRunner
import com.weibo.rill.flow.olympicene.traversal.runners.TimeCheckRunner
import org.slf4j.Logger
import spock.lang.Specification

import java.util.function.BiConsumer
import java.util.function.BiFunction
import java.util.function.Supplier

class CustomizedPluginTest extends Specification {
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Logger mockLogger = Mock(Logger.class)

    def "notify plugin test"() {
        given:
        DAGOperations dagOperationsMock = Mock(DAGOperations.class, 'constructorArgs': [null, null, null, null, null, null, null]) as DAGOperations
        Olympicene olympicene = new Olympicene(dagStorage, dagOperationsMock, SameThreadExecutorService.INSTANCE, null)

        BiConsumer<Runnable, Map<String, Object>> plugin =
                ({
                    nextActions, params ->
                        mockLogger.info("plugin begin execution")
                        if (invokeNextActions) {
                            nextActions.run()
                        }
                } as BiConsumer<Runnable, Map<String, Object>>)
        List plugins = [plugin]
        SystemConfig.NOTIFY_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.NOTIFY_CUSTOMIZED_PLUGINS.addAll(plugins)

        when:
        olympicene.submit('executionId', null, [:])

        then:
        1 * mockLogger.info("plugin begin execution")
        submitInvokeTimes * dagOperationsMock.submitDAG(*_)

        where:
        invokeNextActions | submitInvokeTimes
        false             | 0
        true              | 1
    }

    def "traversal plugin test"() {
        given:
        DAGStorageProcedure dagStorageProcedureMock = Mock(DAGStorageProcedure.class)
        DAGTraversal dagTraversal = new DAGTraversal(dagStorage, dagStorage, dagStorageProcedureMock, SameThreadExecutorService.INSTANCE)

        BiConsumer<Runnable, Map<String, Object>> plugin =
                ({
                    nextActions, params ->
                        mockLogger.info("plugin begin execution")
                        if (invokeNextActions) {
                            nextActions.run()
                        }
                } as BiConsumer<Runnable, Map<String, Object>>)
        List plugins = [plugin]
        SystemConfig.TRAVERSAL_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.TRAVERSAL_CUSTOMIZED_PLUGINS.addAll(plugins)

        when:
        dagTraversal.submitTraversal('executionId', null)

        then:
        1 * mockLogger.info("plugin begin execution")
        submitInvokeTimes * dagStorageProcedureMock.lockAndRun(*_)

        where:
        invokeNextActions | submitInvokeTimes
        false             | 0
        true              | 1
    }

    def "runTask doCollect finishDAG plugin test"() {
        given:
        DAGRunner dagRunnerMock = Mock(DAGRunner.class, 'constructorArgs': [null, null, null]) as DAGRunner
        FunctionTaskRunner functionTaskRunnerMock = Mock(FunctionTaskRunner.class, 'constructorArgs': [null, null, null, null, null, null]) as FunctionTaskRunner
        Map<String, TaskRunner> taskRunners = [(TaskCategory.FUNCTION.getValue()): functionTaskRunnerMock]
        TimeCheckRunner timeCheckRunner = Mock(TimeCheckRunner.class, 'constructorArgs':[null, null, null, null]) as TimeCheckRunner
        DAGTraversal dagTraversal = Mock(DAGTraversal.class, 'constructorArgs': [null, null, null, null]) as DAGTraversal
        DAGOperations dagOperations = new DAGOperations(SameThreadExecutorService.INSTANCE, taskRunners, dagRunnerMock, timeCheckRunner, dagTraversal, Mock(Callback.class), null)

        BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult> plugin =
                ({ nextActions, params ->
                    mockLogger.info("plugin begin execution")
                    if (invokeNextActions) {
                        nextActions.get()
                    }
                    return ExecutionResult.builder().dagInfo(new DAGInfo()).build()
                } as BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult>)
        List plugins = [plugin]
        SystemConfig.TASK_RUN_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.TASK_RUN_CUSTOMIZED_PLUGINS.addAll(plugins)
        SystemConfig.DAG_FINISH_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.DAG_FINISH_CUSTOMIZED_PLUGINS.addAll(plugins)

        FunctionTask functionTask = DAGTraversalSerializer.MAPPER.readValue('{"category":"function"}', FunctionTask.class)
        TaskInfo taskInfo = new TaskInfo()
        taskInfo.setTask(functionTask)

        when:
        dagOperations.runTask('executionId', taskInfo, [:])
        dagOperations.finishDAG('executionId', null, null, null)

        then:
        2 * mockLogger.info("plugin begin execution")
        submitInvokeTimes * functionTaskRunnerMock.run(*_)
        submitInvokeTimes * dagRunnerMock.finishDAG(*_)

        where:
        invokeNextActions | submitInvokeTimes
        false             | 0
        true              | 1
    }

    def cleanup() {
        SystemConfig.NOTIFY_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.TRAVERSAL_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.TASK_RUN_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.TASK_FINISH_CUSTOMIZED_PLUGINS.clear()
        SystemConfig.DAG_FINISH_CUSTOMIZED_PLUGINS.clear()
    }
}
