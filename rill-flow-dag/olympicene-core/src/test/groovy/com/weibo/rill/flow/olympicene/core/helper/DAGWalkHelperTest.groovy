package com.weibo.rill.flow.olympicene.core.helper


import com.weibo.rill.flow.olympicene.core.model.task.ForeachTask
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
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
        ForeachTask baseTask = new ForeachTask('base_1', TaskCategory.FOREACH.getValue(), null, null, null, null, null, null, null, null, null, false, null, null, null)

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
}
