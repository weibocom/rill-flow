package com.weibo.rill.flow.olympicene.traversal.runners;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SwitchTaskRunner extends AbstractTaskRunner {
    private static final String DEFAULT_CONDITION = "default";

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

        // 更新当前节点状态和完成事件
        taskInfo.setTaskStatus(TaskStatus.SUCCEED);
        updateTaskInvokeEndTime(taskInfo);

        dagInfoStorage.saveTaskInfos(executionId, Set.of(taskInfo));
        log.info("run switch task completed, executionId:{}, taskInfoName:{}", executionId, taskInfo.getName());

        return ExecutionResult.builder().taskStatus(taskInfo.getTaskStatus()).build();
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
        DefaultSwitch defaultSwitch = new DefaultSwitch();
        switches.forEach(it -> {
            if (DEFAULT_CONDITION.equals(it.getCondition())) {
                defaultSwitch.setDefaultCondition(it);
            } else {
                boolean condition = calculateCondition(taskInfo, input, it, skipTaskNames, runTaskNames, defaultSwitch);
                // 只要有一个 condition 命中，则不需要执行 default 节点
                if (condition) {
                    defaultSwitch.setNeedDefault(false);
                }
                // 如果当前 condition 命中，并且设置了 break 属性，则不需要执行后续的 condition
                if (condition && it.isBreak()) {
                    defaultSwitch.setBroken(true);
                }
            }
        });
        // 只要 default 存在，就需要调用
        if (defaultSwitch.getDefaultCondition() != null) {
            calculateCondition(taskInfo, input, defaultSwitch.getDefaultCondition(), skipTaskNames, runTaskNames, defaultSwitch);
        }
        // 如果多个 condition 共用了 next 节点，只要有任何一个 condition 命中，则该 next 节点就应该被执行
        // 因此删除 skipTaskNames 中与 runTaskNames 重合的节点名称
        runTaskNames.forEach(skipTaskNames::remove);
        return skipTaskNames;
    }

    @Data
    private static class DefaultSwitch {
        private Switch defaultCondition;
        private boolean needDefault = true;
        private boolean isBroken = false;
    }

    /**
     * 计算单个 condition，将需要跳过的节点名称加入到 skipTaskNames 中，将不需要跳过的节点名称加入到 runTaskNames 中
     *
     * @param taskInfo      当前 switch 节点
     * @param input         当前 switch 节点的输入
     * @param switchObj     单个 condition
     * @param skipTaskNames 需要跳过的节点名称集合
     * @param runTaskNames  不需要跳过的节点名称集合
     * @param defaultSwitch
     * @return
     */
    private static boolean calculateCondition(TaskInfo taskInfo, Map<String, Object> input, Switch switchObj,
                                              Set<String> skipTaskNames, Set<String> runTaskNames, DefaultSwitch defaultSwitch) {
        Set<String> nextTaskNames = Arrays.stream(switchObj.getNext().split(",")).map(String::trim)
                .filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        boolean condition = false;
        condition = judgeCondition(taskInfo, input, switchObj, condition, defaultSwitch);

        if (!condition) {
            skipTaskNames.addAll(nextTaskNames);
        } else {
            runTaskNames.addAll(nextTaskNames);
        }
        return condition;
    }

    private static boolean judgeCondition(TaskInfo taskInfo, Map<String, Object> input, Switch switchObj, boolean condition, DefaultSwitch defaultSwitch) {
        // 此前的 condition 已经 break，则不执行当前 condition
        if (defaultSwitch.isBroken()) {
            return false;
        }
        // 如果 condition 为 default 则根据是否需要 default，返回 true 或 false
        if (DEFAULT_CONDITION.equals(switchObj.getCondition())) {
            return defaultSwitch.isNeedDefault();
        }
        try {
            List<String> result = JsonPath.using(ConditionsUtil.valuePathConf)
                    .parse(ImmutableMap.of("input", input)).read(switchObj.getCondition());
            condition = !result.isEmpty();
        } catch (Exception e) {
            log.warn("switchTask {} evaluation condition expression {} exception. ",
                    taskInfo.getName(), switchObj.getCondition(), e);
        }
        return condition;
    }
}
