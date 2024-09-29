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
        ForeachTask baseTask = new ForeachTask('base_1', '', '',TaskCategory.FOREACH.getValue(), null, null, null, null, null, null, null, null, null, false, null, null, null, null)

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

    def "test getReadyToRunTasks is key mode"() {
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
        taskInfoA.setNext([taskInfoC])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoA, taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        !ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks with stream input and key mode"() {
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
        taskC.getInputType() >> "stream"
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        taskD.isKeyCallback() >> true
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoC])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoA, taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        !ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks with stream input is key mode"() {
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
        taskC.getInputType() >> "stream"
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        taskC.isKeyCallback() >> true
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        taskD.isKeyCallback() >> true
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoC])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoA, taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        !ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks without stream input task"() {
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
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        !ret.contains(taskInfoC)
        !ret.contains(taskInfoB)
        !ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks stream input after func"() {
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
        taskC.getInputType() >> "stream"
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoC)
        !ret.contains(taskInfoB)
        !ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks return before stream input task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.RETURN.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getInputType() >> "stream"
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC])
        then:
        ret.contains(taskInfoA)
        !ret.contains(taskInfoC)
        !ret.contains(taskInfoB)
    }

    def "test getReadyToRunTasks stream input depends on return and function"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.RETURN.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getInputType() >> "stream"
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        taskInfoA.setNext([taskInfoC])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoA, taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC])
        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoC)
        ret.contains(taskInfoB)
    }

    def "test getReadyToRunTasks stream input depends on return"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.RETURN.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getCategory() >> TaskCategory.RETURN.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getInputType() >> "stream"
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setNext([taskInfoD])
        taskInfoB.setDependencies([taskInfoA, taskInfoC])
        taskInfoC.setNext([taskInfoB])
        taskInfoD.setDependencies([taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks stream input after stream input task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getInputType() >> "stream"
        taskB.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getInputType() >> "stream"
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        !ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks stream input after success stream input task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getInputType() >> "stream"
        taskB.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.SUCCEED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.NOT_STARTED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getInputType() >> "stream"
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA])
        taskInfoB.setNext([taskInfoC])
        taskInfoC.setDependencies([taskInfoB])
        taskInfoC.setNext([taskInfoD])
        taskInfoD.setDependencies([taskInfoC])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks stream input depends on stream input task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getInputType() >> "stream"
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.NOT_STARTED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getInputType() >> "stream"
        taskB.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.RETURN.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.SUCCEED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA, taskInfoC, taskInfoD])
        taskInfoC.setNext([taskInfoB])
        taskInfoD.setNext([taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        ret.contains(taskInfoA)
        !ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }

    def "test getReadyToRunTasks stream input depends on success stream input task"() {
        given:
        BaseTask taskA = Mock(BaseTask)
        taskA.getInputType() >> "stream"
        taskA.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskA.getName() >> "A"
        TaskInfo taskInfoA = new TaskInfo(name: "A", taskStatus: TaskStatus.SUCCEED, task: taskA)
        BaseTask taskB = Mock(BaseTask)
        taskB.getInputType() >> "stream"
        taskB.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskB.getName() >> "B"
        TaskInfo taskInfoB = new TaskInfo(name: "B", taskStatus: TaskStatus.NOT_STARTED, task: taskB)
        BaseTask taskC = Mock(BaseTask)
        taskC.getCategory() >> TaskCategory.RETURN.getValue()
        taskC.getName() >> "C"
        TaskInfo taskInfoC = new TaskInfo(name: "C", taskStatus: TaskStatus.SUCCEED, task: taskC)
        BaseTask taskD = Mock(BaseTask)
        taskD.getCategory() >> TaskCategory.FUNCTION.getValue()
        taskD.getName() >> "D"
        TaskInfo taskInfoD = new TaskInfo(name: "D", taskStatus: TaskStatus.NOT_STARTED, task: taskD)
        taskInfoA.setNext([taskInfoB])
        taskInfoB.setDependencies([taskInfoA, taskInfoC, taskInfoD])
        taskInfoC.setNext([taskInfoB])
        taskInfoD.setNext([taskInfoB])

        when:
        Set<TaskInfo> ret = DAGWalkHelper.getInstance().getReadyToRunTasks([taskInfoA, taskInfoB, taskInfoC, taskInfoD])
        then:
        !ret.contains(taskInfoA)
        ret.contains(taskInfoB)
        !ret.contains(taskInfoC)
        ret.contains(taskInfoD)
    }
}
