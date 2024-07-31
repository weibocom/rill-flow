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
        if (CollectionUtils.isEmpty(switches)) {
            TaskInvokeMsg taskInvokeMsg = TaskInvokeMsg.builder().msg("switches collection empty").build();
            taskInfo.updateInvokeMsg(taskInvokeMsg);
            updateTaskInvokeEndTime(taskInfo);
            taskInfo.setTaskStatus(TaskStatus.SUCCEED);
            dagInfoStorage.saveTaskInfos(executionId, ImmutableSet.of(taskInfo));
            return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
        }


        Set<String> skipTaskNames = new HashSet<>();
        calculateConditions(taskInfo, input, switches, skipTaskNames);
        switchTask.getSkipNextTaskNames().addAll(skipTaskNames);

        Set<TaskInfo> taskInfosNeedToUpdate = Sets.newHashSet();
        List<TaskInfo> nextTasks = taskInfo.getNext();
        Set<TaskInfo> skippedNextTasks = new HashSet<>();
        Set<String> skippedTaskNames = new HashSet<>();
        for (TaskInfo nextTask : nextTasks) {
            if (skipTaskNames.contains(nextTask.getName())) {
                boolean taskNeedSkip = Optional.ofNullable(nextTask.getDependencies())
                        .filter(dependedTasks -> dependedTasks.stream()
                                .filter(Objects::nonNull)
                                .allMatch(dependedTask -> dependedTask.getName().equals(taskInfo.getName())))
                        .isPresent();
                if (taskNeedSkip) {
                    nextTask.setTaskStatus(TaskStatus.SKIPPED);
                    taskInfosNeedToUpdate.add(nextTask);
                    skippedNextTasks.add(nextTask);
                    skippedTaskNames.add(nextTask.getName());
                }
            }
        }
        for (TaskInfo skipTask : skippedNextTasks) {
            Map<String, TaskInfo> siblingTaskInfos = getSiblingTaskInfoMap(executionId, skipTask);
            List<TaskInfo> currentTaskNext = Optional.ofNullable(siblingTaskInfos.get(skipTask.getName())).map(TaskInfo::getNext).orElse(null);
            setNextTaskSkipStatus(siblingTaskInfos.size(), taskInfosNeedToUpdate, currentTaskNext, skippedTaskNames);
        }
        taskInfo.setTaskStatus(TaskStatus.SUCCEED);
        updateTaskInvokeEndTime(taskInfo);
        taskInfosNeedToUpdate.add(taskInfo);

        dagInfoStorage.saveTaskInfos(executionId, taskInfosNeedToUpdate);
        log.info("run switch task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
    }

    private static void calculateConditions(TaskInfo taskInfo, Map<String, Object> input, List<Switch> switches,
                                            Set<String> skipTaskNames) {
        Set<String> runTaskNames = new HashSet<>();
        switches.stream()
                .sorted((a, b) -> a.getCondition().compareToIgnoreCase(b.getCondition()))
                .forEach(it -> {
                    if (it.getNext() == null) {
                        return;
                    }
                    Set<String> nextTaskNames = Arrays.stream(it.getNext().split(",")).map(String::trim)
                            .filter(StringUtils::isNotBlank).collect(Collectors.toSet());
                    if (nextTaskNames.isEmpty()) {
                        return;
                    }

                    boolean condition = false;
                    try {
                        List<String> result = JsonPath.using(ConditionsUtil.valuePathConf).parse(ImmutableMap.of("input", input)).read(it.getCondition());
                        condition = !result.isEmpty();
                    } catch (Exception e) {
                        log.warn("switchTask {} evaluation condition expression {} exception. ", taskInfo.getName(), it.getCondition(), e);
                    }

                    if (!condition) {
                        skipTaskNames.addAll(nextTaskNames);
                    } else {
                        runTaskNames.addAll(nextTaskNames);
                    }
                });
        // 如果多个 condition 共用了 next 节点，只要有任何一个 condition 命中，则该 next 节点就应该被执行
        runTaskNames.forEach(skipTaskNames::remove);
    }
}
