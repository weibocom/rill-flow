package com.weibo.rill.flow.olympicene.traversal.runners;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.jayway.jsonpath.JsonPath;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import com.weibo.rill.flow.olympicene.core.model.task.Switch;
import com.weibo.rill.flow.olympicene.core.model.task.SwitchTask;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.utils.ConditionsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SwitchTaskRunner extends AbstractTaskRunner {
    public SwitchTaskRunner(InputOutputMapping inputOutputMapping, DAGInfoStorage dagInfoStorage,
                            DAGContextStorage dagContextStorage, DAGStorageProcedure dagStorageProcedure,
                            SwitcherManager switcherManager) {
        super(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Override
    public TaskCategory getCategory() {
        return TaskCategory.SWITCH;
    }

    @Override
    public String getIcon() {
        return "ant-design:branches-outlined";
    }

    @Override
    protected ExecutionResult doRun(String executionId, TaskInfo taskInfo, Map<String, Object> input) {
        log.info("switch task begin to run executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        SwitchTask switchTask = (SwitchTask) taskInfo.getTask();

        List<Switch> switches = switchTask.getSwitches();
        // switches 条件为空，不需要计算，直接更新当前节点状态
        if (CollectionUtils.isEmpty(switches)) {
            TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().msg("switches collection empty").build();
            taskInfo.updateInvokeMsg(taskInvokeMsg);
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
            dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
        }

        // 计算 switch 节点的后继节点中，哪些节点应该被跳过
        // 并将可能需要跳过的节点名称记录在 skipNextTaskNames HashSet 中，避免后继节点同时依赖多个 switch 节点或 return 节点的情况
        // 后继节点同时依赖多个 switch 节点或 return 节点的情况在 AbstractTaskRunner 类的 needNormalSkip 方法中进行判断
        Set<String> skipTaskNames = calculateConditions(taskInfo, input, switches);
        taskInfo.getSkipNextTaskNames().addAll(skipTaskNames);

        // 计算后继及后继的后继节点中需要跳过的节点，更新其状态，并加入 taskInfo 集合返回
        Set<TaskInfo> taskInfosNeedToUpdate = getTaskInfosNeedToUpdate(taskInfo, skipTaskNames);

        // 更新当前节点状态
        taskInfo.setTaskStatus(TaskStatus.SUCCEED);
        updateTaskInvokeEndTime(taskInfo);
        taskInfosNeedToUpdate.add(taskInfo);

        // 批量写入存储，持久化节点状态
        dagInfoStorage.saveTaskInfos(executionId, taskInfosNeedToUpdate);
        log.info("run switch task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
    }

    private Set<TaskInfo> getTaskInfosNeedToUpdate(TaskInfo taskInfo, Set<String> skipTaskNames) {
        Set<TaskInfo> taskInfosNeedToUpdate = Sets.newHashSet();
        List<TaskInfo> nextTasks = taskInfo.getNext();
//        Set<TaskInfo> skippedNextTasks = new HashSet<>();
//        Set<String> skippedTaskNames = new HashSet<>();
        for (TaskInfo nextTask : nextTasks) {
            if (!skipTaskNames.contains(nextTask.getName())) {
                continue;
            }
            // 只依赖当前节点，且根据 condition 判断需要被跳过的节点，则将其状态设置为 SKIPPED
            // 除此以外还依赖了其他节点且需要被跳过的节点，在 AbstractTaskRunner 类的 needNormalSkip 方法中进行判断
            boolean taskNeedSkip = Optional.ofNullable(nextTask.getDependencies())
                    .filter(dependedTasks -> dependedTasks.stream()
                            .filter(Objects::nonNull)
                            .allMatch(dependedTask -> dependedTask.getName().equals(taskInfo.getName())))
                    .isPresent();
            if (!taskNeedSkip) {
                continue;
            }
            nextTask.setTaskStatus(TaskStatus.SKIPPED);
            taskInfosNeedToUpdate.add(nextTask);
//            skippedNextTasks.add(nextTask);
//            skippedTaskNames.add(nextTask.getName());
        }

        // 递归处理后继的后继们，将需要被跳过的节点的状态设置为 SKIPPED 并加入到 taskInfosNeedToUpdate
//        for (TaskInfo skipTask : skippedNextTasks) {
//            Map<String, TaskInfo> siblingTaskInfos = getSiblingTaskInfoMap(executionId, skipTask);
//            List<TaskInfo> currentTaskNext = Optional.ofNullable(siblingTaskInfos.get(skipTask.getName()))
//                    .map(TaskInfo::getNext).orElse(null);
//            setNextTaskSkipStatus(siblingTaskInfos.size(), taskInfosNeedToUpdate, currentTaskNext, skippedTaskNames);
//        }
        return taskInfosNeedToUpdate;
    }

    /**
     * 计算各条件，判断哪些节点需要 skip 不执行
     * @param taskInfo 当前 switch 节点
     * @param input 当前 switch 节点的输入
     * @param switches switch 节点的所有 condition
     * @return 需要跳过的节点名称集合
     */
    private static Set<String> calculateConditions(TaskInfo taskInfo, Map<String, Object> input, List<Switch> switches) {
        Set<String> skipTaskNames = new HashSet<>();
        Set<String> runTaskNames = new HashSet<>();
        switches.forEach(it -> calculateCondition(taskInfo, input, it, skipTaskNames, runTaskNames));
        // 如果多个 condition 共用了 next 节点，只要有任何一个 condition 命中，则该 next 节点就应该被执行
        // 因此删除 skipTaskNames 中与 runTaskNames 重合的节点名称
        runTaskNames.forEach(skipTaskNames::remove);
        return skipTaskNames;
    }

    /**
     * 计算单个 condition，将需要跳过的节点名称加入到 skipTaskNames 中，将不需要跳过的节点名称加入到 runTaskNames 中
     * @param taskInfo 当前 switch 节点
     * @param input 当前 switch 节点的输入
     * @param switchObj 单个 condition
     * @param skipTaskNames 需要跳过的节点名称集合
     * @param runTaskNames 不需要跳过的节点名称集合
     */
    private static void calculateCondition(TaskInfo taskInfo, Map<String, Object> input, Switch switchObj,
                                           Set<String> skipTaskNames, Set<String> runTaskNames) {
        // 如果当前条件没有需要执行的节点，则无需进行计算
        if (StringUtils.isBlank(switchObj.getNext())) {
            return;
        }
        Set<String> nextTaskNames = Arrays.stream(switchObj.getNext().split(",")).map(String::trim)
                .filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        if (nextTaskNames.isEmpty()) {
            return;
        }

        boolean condition = false;
        try {
            List<String> result = JsonPath.using(ConditionsUtil.valuePathConf)
                    .parse(ImmutableMap.of("input", input)).read(switchObj.getCondition());
            condition = !result.isEmpty();
        } catch (Exception e) {
            log.warn("switchTask {} evaluation condition expression {} exception. ",
                    taskInfo.getName(), switchObj.getCondition(), e);
        }

        if (!condition) {
            skipTaskNames.addAll(nextTaskNames);
        } else {
            runTaskNames.addAll(nextTaskNames);
        }
    }
}
