package com.weibo.rill.flow.olympicene.core.helper

import com.weibo.rill.flow.interfaces.model.task.BaseTask
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.core.model.task.ForeachTask
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import spock.lang.Specification

class DAGWalkHelperTest extends Specification {

    static TaskInfo succeedTask = new TaskInfo(taskStatus: TaskStatus.SUCCEED, name: "succeedTask")
    static TaskInfo failedTask = new TaskInfo(taskStatus: TaskStatus.FAILED, name: "failedTask")
    static TaskInfo skippedTask = new TaskInfo(taskStatus: TaskStatus.SKIPPED, name: "skippedTask")
    static TaskInfo runningTask = new TaskInfo(taskStatus: TaskStatus.RUNNING, name: "runningTask")
    static TaskInfo keySucceedTask = new TaskInfo(taskStatus: TaskStatus.KEY_SUCCEED, name: "keySucceedTask")
    static TaskInfo stashedTask = new TaskInfo(taskStatus: TaskStatus.STASHED, name: "stashedTask")

    def "task status of parent task should be correct"() {
        given:
        ForeachTask baseTask = new ForeachTask('base_1', '', '',TaskCategory.FOREACH.getValue(), null, null, null, null, null, null, null, null, null, false, null, null, null, null, null, null)

        TaskInfo parentTask = new TaskInfo(name: 'parent',
                task: baseTask,
                subGroupIndexToStatus: subStatus,
                subGroupKeyJudgementMapping: subKeyInfo
        )

        when:
        TaskStatus result = DAGWalkHelper.getInstance().calculateParentStatus(parentTask)

        then:
        expectRes == result

        where:
        subStatus                                              | subKeyInfo              || expectRes
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.RUNNING] | ['0': true, '1': true]  || TaskStatus.RUNNING
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.SUCCEED] | ['0': true, '1': true]  || TaskStatus.KEY_SUCCEED
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.RUNNING] | ['0': true, '1': false] || TaskStatus.KEY_SUCCEED
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.RUNNING] | [:]                     || TaskStatus.RUNNING
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.FAILED]  | ['0': true, '1': true]  || TaskStatus.FAILED
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.FAILED]  | ['0': true, '1': false] || TaskStatus.KEY_SUCCEED
        ['0': TaskStatus.KEY_SUCCEED, '1': TaskStatus.FAILED]  | [:]                     || TaskStatus.FAILED
        ['0': TaskStatus.RUNNING, '1': TaskStatus.RUNNING]     | ['0': true, '1': true]  || TaskStatus.RUNNING
        ['0': TaskStatus.RUNNING, '1': TaskStatus.SUCCEED]     | ['0': true, '1': true]  || TaskStatus.RUNNING
        ['0': TaskStatus.RUNNING, '1': TaskStatus.SUCCEED]     | ['0': true, '1': false] || TaskStatus.RUNNING
        ['0': TaskStatus.SUCCEED, '1': TaskStatus.SUCCEED]     | [:]                     || TaskStatus.SUCCEED
        ['0': TaskStatus.SUCCEED, '1': TaskStatus.FAILED]      | [:]                     || TaskStatus.FAILED
    }


    def "dag status should be correct"() {
        given:
        DAGInfo dagInfo = new DAGInfo();
        dagInfo.setTasks(tasks)

        when:
        DAGStatus dagStatus = DAGWalkHelper.getInstance().calculateDAGStatus(dagInfo)

        then:
        expectRes == dagStatus

        where:
        tasks                                      || expectRes
        ['a': keySucceedTask, 'b': succeedTask]    || DAGStatus.KEY_SUCCEED
        ['a': keySucceedTask, 'b': runningTask]    || DAGStatus.RUNNING
        ['a': keySucceedTask, 'b': stashedTask]    || DAGStatus.KEY_SUCCEED
        ['a': keySucceedTask, 'b': skippedTask]    || DAGStatus.KEY_SUCCEED
        ['a': keySucceedTask, 'b': failedTask]     || DAGStatus.FAILED
        ['a': keySucceedTask, 'b': keySucceedTask] || DAGStatus.KEY_SUCCEED

        ['a': succeedTask, 'b': runningTask]       || DAGStatus.RUNNING
        ['a': succeedTask, 'b': stashedTask]       || DAGStatus.KEY_SUCCEED
        ['a': succeedTask, 'b': skippedTask]       || DAGStatus.SUCCEED
        ['a': succeedTask, 'b': failedTask]        || DAGStatus.FAILED

        ['a': runningTask, 'b': stashedTask]       || DAGStatus.RUNNING
        ['a': runningTask, 'b': skippedTask]       || DAGStatus.RUNNING
        ['a': runningTask, 'b': failedTask]        || DAGStatus.RUNNING

        ['a': skippedTask, 'b': failedTask]        || DAGStatus.FAILED

        [:]                                        || DAGStatus.SUCCEED
    }

    /**
     * 1. 非流式输入，所有依赖全部完成 -> 执行
     * 2. 非流式输入，关键路径依赖完成 -> 执行
     * 3. 非流式输入，有一个 block 输出节点未完成 -> 不执行
     * 4. 非流式输入，有一个 stream 输出节点未完成 -> 不执行
     * 5. 流式输入，依赖的 block 节点均未完成 -> 不执行
     * 6. 流式输入，有一个 block 输出节点完成，其他 block 输出节点未完成 -> 执行
     * 7. 流式输入，所有依赖均未开始执行，但依赖的一个 stream 输出节点可执行 -> 执行
     * 8. 流式输入，所有依赖均未开始执行，依赖的 stream 输出节点也不可执行 -> 不执行
     * 9. 当一条路径上存在多个未执行的流式输入任务时，只有第一个需要被执行
     */

    def "1. test getReadyToRunTasks when non stream input task with all dependencies succeed"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.SUCCEED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.KEY_SUCCEED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        taskInfoA.setNext([taskInfoC])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoA, taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC])
        then:
        !ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        ret.contains(taskInfoC)
    }

    def "2.3. test getReadyToRunTasks when non stream input task with key path dependencies succeed"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.SUCCEED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.KEY_SUCCEED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        BaseTask taskE = Mock(BaseTask)
        taskE.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskE.getName() >> "E"
        TaskInfo taskInfoE = new TaskInfo(name: "E", taskStatus: TaskStatus.SUCCEED, task: taskE)
        taskInfoA.setNext([taskInfoC])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoA, taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC, taskInfoE])
        taskInfoE.setNext([taskInfoD])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD, taskInfoE])
        then:
        !ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
        !ret.contains(taskInfoE)
    }

    def "4. test getReadyToRunTasks when non stream input task depends on unfinished stream output task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.SUCCEED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.SUCCEED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.getOutputType() >> "stream"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.RUNNING, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoD])
        taskInfoB.setNext([taskInfoD])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoA, taskInfoB, taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        !ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
    }

    def "5. test getReadyToRunTasks when stream input task with all block output dependencies unfinished"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        taskD.getInputType() >> "stream"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoD])
        taskInfoB.setNext([taskInfoD])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoA, taskInfoB, taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
    }

    def "6. test getReadyToRunTasks when stream input task with one block output dependency succeed"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.SUCCEED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        taskD.getInputType() >> "stream"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoD])
        taskInfoB.setNext([taskInfoD])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoA, taskInfoB, taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        !ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }

    def "7. test getReadyToRunTasks when stream input task depends on ready to run stream output task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.getOutputType() >> "stream"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        taskD.getInputType() >> "stream"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoD])
        taskInfoB.setNext([taskInfoD])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoA, taskInfoB, taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }

    def "8. test getReadyToRunTasks when stream input task depends on unready stream output task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.getOutputType() >> "stream"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        taskD.getInputType() >> "stream"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        BaseTask taskE = Mock(BaseTask)
        taskE.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskE.getName() >> "E"
        TaskInfo taskInfoE = new TaskInfo(name: "E", taskStatus: TaskStatus.NOT_STARTED, task: taskE)
        taskInfoA.setNext([taskInfoD])
        taskInfoB.setNext([taskInfoD])
        taskInfoC.setDependencies([taskInfoE])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoA, taskInfoB, taskInfoC])
        taskInfoE.setNext([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD, taskInfoE])

        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
        ret.contains(taskInfoE)
    }

    def "9. test getReadyToRunTasks when there are multiple stream input tasks"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        taskA.getOutputType() >> "stream"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.SUSPENSE.getValue()
        taskB.getName() >> "B"
        taskB.getOutputType() >> "stream"
        taskB.getInputType() >> "stream"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.getInputType() >> "stream"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setNext([taskInfoC])
        taskInfoB.setDependencies([taskInfoA])
        taskInfoC.setDependencies([taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC])

        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
    }
}
