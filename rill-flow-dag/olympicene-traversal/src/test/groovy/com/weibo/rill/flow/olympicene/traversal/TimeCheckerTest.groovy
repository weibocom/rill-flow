package com.weibo.rill.flow.olympicene.traversal

import com.google.common.collect.Sets
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient
import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.ForeachTaskValidator
import com.weibo.rill.flow.olympicene.ddl.validation.task.impl.FunctionTaskValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.checker.TimeCheckMember
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import com.weibo.rill.flow.olympicene.traversal.runners.TimeCheckRunner
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import spock.lang.Specification


class TimeCheckerTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator([new FunctionTaskValidator(), new ForeachTaskValidator()])])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    DefaultTimeChecker timeChecker = new DefaultTimeChecker()
    Olympicene olympicene
    String executionId = 'executionId'

    def setup() {
        timeChecker.redisClient = Mock(RedisClient.class)
        SwitcherManager switcherManager = Mock(SwitcherManager.class)
        olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, timeChecker, switcherManager)
        timeChecker.timeCheckRunner = olympicene.dagOperations.timeCheckRunner
    }

    def "function type task timeout check"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: function://olympicene::test::funtion1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: 1\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)
        timeChecker.redisClient.zrangeByScore('all_time_check_redis_key', *_) >> Sets.newHashSet('time_check')
        TimeCheckMember member = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.TASK_TIMEOUT_CHECK)
                .executionId(executionId)
                .taskCategory(TaskCategory.FUNCTION.getValue())
                .taskInfoName('A')
                .build()
        timeChecker.redisClient.eval(*_) >> [DAGTraversalSerializer.serializeToString(member).getBytes()] >> null

        when:
        olympicene.submit(executionId, dag, [:])
        timeChecker.timeCheck()

        then:
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(member))
        1 * timeChecker.redisClient.zadd('all_time_check_redis_key', _, 'time_check')
        1 * timeChecker.redisClient.zrem('time_check', DAGTraversalSerializer.serializeToString(member))
        1 * callback.onEvent({
            Event event ->
                event.getEventCode() == DAGEvent.TASK_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getMsg() == 'timeout'
        })
    }

    def "suspense type task timeout check"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: 1\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.url\n" +
                "       source: \$.context.url\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n"
        DAG dag = dagParser.parse(text)
        timeChecker.redisClient.zrangeByScore('all_time_check_redis_key', *_) >> Sets.newHashSet('time_check')
        TimeCheckMember member = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.TASK_TIMEOUT_CHECK)
                .executionId(executionId)
                .taskCategory(TaskCategory.SUSPENSE.getValue())
                .taskInfoName('A')
                .build()
        timeChecker.redisClient.eval(*_) >> [DAGTraversalSerializer.serializeToString(member).getBytes()] >> null

        when:
        olympicene.submit(executionId, dag, [:])
        timeChecker.timeCheck()

        then:
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(member))
        1 * timeChecker.redisClient.zadd('all_time_check_redis_key', _, 'time_check')
        1 * timeChecker.redisClient.zrem('time_check', DAGTraversalSerializer.serializeToString(member))
        1 * callback.onEvent({
            Event event ->
                event.getEventCode() == DAGEvent.TASK_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getMsg() == 'timeout'
        })
    }

    def "dag timeout check"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "timer:\n" +
                "  timeoutInSeconds: 1\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: function://olympicene::test::funtion1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)
        timeChecker.redisClient.zrangeByScore('all_time_check_redis_key', *_) >> Sets.newHashSet('time_check')
        TimeCheckMember member = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.DAG_TIMEOUT_CHECK)
                .executionId(executionId)
                .build()
        timeChecker.redisClient.eval(*_) >> [DAGTraversalSerializer.serializeToString(member).getBytes()] >> null

        when:
        olympicene.submit(executionId, dag, [:])
        timeChecker.timeCheck()

        then:
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(member))
        1 * timeChecker.redisClient.zadd('all_time_check_redis_key', _, 'time_check')
        1 * timeChecker.redisClient.zrem('time_check', DAGTraversalSerializer.serializeToString(member))
        1 * callback.onEvent({
            Event event ->
                event.getEventCode() == DAGEvent.DAG_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getMsg() == 'timeout'
        })
    }

    def "dag timeout before task timeout"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "timer:\n" +
                "  timeoutInSeconds: 1\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: function://olympicene::test::funtion1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: 2\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)
        timeChecker.redisClient.zrangeByScore('all_time_check_redis_key', *_) >> Sets.newHashSet('time_check')
        TimeCheckMember dagMember = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.DAG_TIMEOUT_CHECK)
                .executionId(executionId)
                .build()
        TimeCheckMember taskMember = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.TASK_TIMEOUT_CHECK)
                .executionId(executionId)
                .taskCategory(TaskCategory.FUNCTION.getValue())
                .taskInfoName('A')
                .build()
        timeChecker.redisClient.eval(*_) >> [DAGTraversalSerializer.serializeToString(dagMember).getBytes()] >> null

        when:
        olympicene.submit(executionId, dag, [:])
        timeChecker.timeCheck()

        then:
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(dagMember))
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(taskMember))
        2 * timeChecker.redisClient.zadd('all_time_check_redis_key', _, 'time_check')
        1 * timeChecker.redisClient.zrem('time_check', DAGTraversalSerializer.serializeToString(dagMember))
        1 * callback.onEvent({
            Event event ->
                event.getEventCode() == DAGEvent.DAG_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getMsg() == 'timeout' &&
                        ((DAGCallbackInfo) event.getData()).getDagInfo().getTask('A').getTaskStatus() == TaskStatus.RUNNING
        })
    }

    def "dag timeout after task timeout"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "timer:\n" +
                "  timeoutInSeconds: 2\n" +
                "tasks: \n" +
                "- category: function\n" +
                "  name: A\n" +
                "  resourceName: function://olympicene::test::funtion1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: 1\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url\n" +
                "- category: function\n" +
                "  name: B\n" +
                "  resourceName: function://olympicene::test::funtion1::prod\n" +
                "  pattern: task_scheduler\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.gopUrls\n" +
                "       source: \$.context.gopUrls\n" +
                "  outputMappings:\n" +
                "     - target: \$.context.url\n" +
                "       source: \$.output.url"
        DAG dag = dagParser.parse(text)
        timeChecker.redisClient.zrangeByScore('all_time_check_redis_key', *_) >> Sets.newHashSet('time_check')
        TimeCheckMember dagMember = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.DAG_TIMEOUT_CHECK)
                .executionId(executionId)
                .build()
        TimeCheckMember taskAMember = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.TASK_TIMEOUT_CHECK)
                .executionId(executionId)
                .taskCategory(TaskCategory.FUNCTION.getValue())
                .taskInfoName('A')
                .build()
        TimeCheckMember taskBMember = TimeCheckMember.builder()
                .checkMemberType(TimeCheckMember.CheckMemberType.TASK_TIMEOUT_CHECK)
                .executionId(executionId)
                .taskCategory(TaskCategory.FUNCTION.getValue())
                .taskInfoName('B')
                .build()
        timeChecker.redisClient.eval(*_) >> [DAGTraversalSerializer.serializeToString(taskAMember).getBytes()] >> null >> [DAGTraversalSerializer.serializeToString(dagMember).getBytes()] >> null

        when:
        olympicene.submit(executionId, dag, [:])
        timeChecker.timeCheck()
        timeChecker.timeCheck()

        then:
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(dagMember))
        1 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(taskAMember))
        0 * timeChecker.redisClient.zadd('time_check', _, DAGTraversalSerializer.serializeToString(taskBMember))
        2 * timeChecker.redisClient.zadd('all_time_check_redis_key', _, 'time_check')
        1 * timeChecker.redisClient.zrem('time_check', DAGTraversalSerializer.serializeToString(dagMember))
        1 * timeChecker.redisClient.zrem('time_check', DAGTraversalSerializer.serializeToString(taskAMember))
        1 * callback.onEvent({
            Event event ->
                event.getEventCode() == DAGEvent.TASK_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getTaskInfo().getName() == 'A' &&
                        ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskInvokeMsg().getMsg() == 'timeout'
        })
        1 * callback.onEvent({
            Event event ->
                event.getEventCode() == DAGEvent.DAG_FAILED.getCode() &&
                        ((DAGCallbackInfo) event.getData()).getDagInfo().getDagInvokeMsg().getMsg() == 'timeout' &&
                        ((DAGCallbackInfo) event.getData()).getDagInfo().getTask('B').getTaskStatus() == TaskStatus.RUNNING
        })
    }

    def "dag timeout config value check"() {
        given:
        String text = "version: 0.0.1\n" +
                "namespace: olympicene\n" +
                "service: mca\n" +
                "name: test\n" +
                "type: flow\n" +
                "timer:\n" +
                "  timeoutInSeconds: 1\n" +
                "tasks: \n" +
                "- category: suspense\n" +
                "  name: A\n" +
                "  timeline:\n" +
                "    timeoutInSeconds: 1\n" +
                "  conditions:\n" +
                "    - \$.input.[?(@.url == \"bbb\")]\n" +
                "  inputMappings:\n" +
                "     - target: \$.input.taskTimeout\n" +
                "       source: \$.context.taskTimeout"
        DAG dag = dagParser.parse(text)
        dag.getTimeline().setTimeoutInSeconds(dagTimeout)
        dag.getTasks().get(0).getTimeline().setTimeoutInSeconds(taskTimeout)

        TimeCheckRunner timeCheckRunner = Mock(TimeCheckRunner.class, 'constructorArgs': [null, null, null, null]) as TimeCheckRunner
        DAGOperations dagOperations = new DAGOperations(olympicene.dagOperations.runnerExecutor, olympicene.dagOperations.taskRunners, olympicene.dagOperations.dagRunner,
                timeCheckRunner, olympicene.dagOperations.dagTraversal, olympicene.dagOperations.callback, olympicene.dagOperations.dagResultHandler)
        dagOperations.dagTraversal.setDagOperations(dagOperations)
        Olympicene olympiceneTimeMock = new Olympicene(olympicene.dagInfoStorage, dagOperations, olympicene.notifyExecutor, olympicene.dagResultHandler)

        when:
        olympiceneTimeMock.submit(executionId, dag, ['dagTimeout': '3', 'taskTimeout': '3'])

        then:
        1 * timeCheckRunner.addDAGToTimeoutCheck(executionId, timeoutValue)
        1 * timeCheckRunner.addTaskToTimeoutCheck(executionId, _, timeoutValue)

        where:
        dagTimeout              | taskTimeout            | timeoutValue
        "2"                     | "2"                    | 2
        "\$.context.dagTimeout" | "\$.input.taskTimeout" | 3
    }
}