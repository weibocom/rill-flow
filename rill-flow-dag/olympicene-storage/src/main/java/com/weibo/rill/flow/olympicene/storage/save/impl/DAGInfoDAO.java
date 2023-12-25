/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.olympicene.storage.save.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.task.BaseTask;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.constant.SystemConfig;
import com.weibo.rill.flow.olympicene.core.exception.SerializationException;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.helper.TaskInfoMaker;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.storage.constant.DAGRedisPrefix;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.script.RedisScriptManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <pre>
 * 存储结构说明
 * 1. 目标
 *    每个任务能够单独更新 不因并发问题导致互相覆盖 如 A:{children:{B:xxx, C:xxx}}
 *    若将A对应的taskInfo bean序列化为一个字符串，则任务B C 存储在一个字符串中, 更新时可能会互相覆盖
 *
 * 2. DAGInfo bean举例
 *    DAGInfo bean中有任务A, A任务有子任务B, B任务有子任务C
 *    {
 *      "execution_id": "id",
 *      "dag": xxx,
 *      "dag_status": "running",
 *      "tasks": {
 *        "A": {
 *          "task": xxx,
 *          "name": "A",
 *          "task_status": "SUCCEED",
 *          "children": {
 *            "B": {
 *              "task": xxx,
 *              "name": "B",
 *              "task_status": "SUCCEED",
 *              "children": {
 *                "C": {
 *                  "task": xxx,
 *                  "name": "C",
 *                  "task_status": "SUCCEED"
 *                }
 *              }
 *            }
 *          }
 *        }
 *      }
 *    }
 *
 * 3. redis 存储内容说明
 *   3.1 DAGInfo
 *       类型: hash
 *       key: dag_info_ + executionId
 *       filed          |  value
 *       "execution_id" | "id"
 *       "dag"          | xxx        value: DAG bean序列化后的字符串
 *       "dag_status"   | "running"
 *       "#A"           | xxx        对于map DAGInfo->tasks
 *                                   field: #+key
 *                                   value: taskInfo中next/parent/children/dependencies设置为空后bean序列化为字符串
 *   3.2 任务与其子任务映射
 *       类型: hash
 *       key: sub_task_mapping_ + executionId
 *       field | value
 *       "A"   | "sub_task_id_A"
 *       "B"   | "sub_task_id_B"   若B有子任务
 *                                 field: B任务名称
 *                                 value: 存储B子任务的redis key
 *   3.3 子任务信息
 *       类型: hash
 *       key: sub_task_ + executionId + _ + 父任务名称, 如: sub_task_id_A
 *       field | value
 *       #B    | xxx     对于map TaskInfo->children
 *                       field: #+key
 *                       value: taskInfo中next/parent/children/dependencies设置为空后bean序列化为字符串
 * </pre>
 *
 * @see ContextDAO
 */
@Slf4j
public class DAGInfoDAO {
    public static final String TASK_FIELD_PREFIX = "#";
    public static final String EXECUTION_ID = "execution_id";
    public static final String DAG_DESCRIBER = "dag";
    public static final String DAG_INVOKE_MSG = "dag_invoke_msg";
    public static final String DAG_STATUS = "dag_status";

    private final RedisClient redisClient;
    private final int finishStatusReserveTimeInSecond;
    private final int unfinishedStatusReserveTimeInSecond;
    private final DAGInfoDeserializeService dagInfoDeserializeService;

    public DAGInfoDAO(RedisClient redisClient, DAGInfoDeserializeService dagInfoDeserializeService) {
        this(redisClient, dagInfoDeserializeService, 2 * 24 * 3600, 0);
    }

    public DAGInfoDAO(RedisClient redisClient, DAGInfoDeserializeService dagInfoDeserializeService,
                      int unfinishedStatusReserveTimeInSecond, int finishStatusReserveTimeInSecond) {
        this.redisClient = redisClient;
        this.dagInfoDeserializeService = dagInfoDeserializeService;
        this.unfinishedStatusReserveTimeInSecond = unfinishedStatusReserveTimeInSecond;
        this.finishStatusReserveTimeInSecond = finishStatusReserveTimeInSecond;
    }

    // ------------------------------------------------------
    // 若想动态修改属性值 如: 不同业务设置不同值
    // 需要继承该类 重写该方法 添加动态设置业务逻辑
    protected void checkDAGInfoLength(String executionId, List<byte[]> contents) {
        log.debug("checkDAGInfoLength executionId:{} contents size empty:{}", executionId, CollectionUtils.isEmpty(contents));
    }

    protected int getFinishStatusReserveTimeInSecond(String executionId) {
        log.debug("getFinishStatusReserveTimeInSecond executionId:{}, time:{}", executionId, finishStatusReserveTimeInSecond);
        return finishStatusReserveTimeInSecond;
    }

    protected int getUnfinishedStatusReserveTimeInSecond(String executionId) {
        log.debug("getUnfinishedStatusReserveTimeInSecond executionId:{}, time:{}", executionId, unfinishedStatusReserveTimeInSecond);
        return unfinishedStatusReserveTimeInSecond;
    }
    // ------------------------------------------------------

    public DAGInfo getDagInfo(String executionId, boolean needSubTasks) {
        try {
            log.info("getDagInfo executionId:{} needSubTasks:{}", executionId, needSubTasks);
            List<List<List<byte[]>>> dagInfos = getDagInfoFromRedis(executionId, needSubTasks);
            if (CollectionUtils.isEmpty(dagInfos)) {
                return null;
            }

            List<byte[]> contents = dagInfos.stream()
                    .map(array -> array.get(1))
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMap(Collection::stream)
                    .toList();
            checkDAGInfoLength(executionId, contents);

            return deserializeDagInfo(dagInfos);
        } catch (Exception e) {
            log.warn("getDagInfo fails, executionId:{}", executionId, e);
            throw e;
        }
    }

    private List<List<List<byte[]>>> getDagInfoFromRedis(String executionId, boolean needSubTasks) {
        List<String> keys = !needSubTasks ?
                Lists.newArrayList(buildDagInfoRedisKey(executionId)) :
                Lists.newArrayList(buildDagInfoRedisKey(executionId), buildTaskNameToSubTaskRedisKey(executionId));
        return (List<List<List<byte[]>>>) redisClient.eval(
                RedisScriptManager.dagInfoGetScript(), executionId, keys, Lists.newArrayList());
    }

    private DAGInfo deserializeDagInfo(List<List<List<byte[]>>> dagInfoByte) {
        DAGInfo dagInfo = dagInfoDeserializeService.deserializeBaseDagInfo(dagInfoByte.get(0));
        if (dagInfo == null) {
            return null;
        }
        appendTask(dagInfo, dagInfoByte);
        appendTaskRelation(1, dagInfo.getTasks());
        return dagInfo;
    }

    private void appendTask(DAGInfo dagInfo, List<List<List<byte[]>>> dagInfoByte) {
        Map<String, BaseTask> baseTaskMap = getBaseTask(1, Optional.ofNullable(dagInfo.getDag()).map(DAG::getTasks).orElse(null));
        Map<String, Map<String, TaskInfo>> taskNameToSubTasks = dagInfoDeserializeService.getTaskNameToSubTasksMap(dagInfoByte);
        doAppendTask(1, dagInfo.getTasks(), baseTaskMap, taskNameToSubTasks);
    }

    private Map<String, BaseTask> getBaseTask(int depth, List<BaseTask> baseTasks) {
        if (CollectionUtils.isEmpty(baseTasks) || depth > SystemConfig.getTaskMaxDepth()) {
            return Maps.newHashMap();
        }

        Map<String, BaseTask> baseTaskMap = Maps.newHashMap();
        baseTasks.forEach(baseTask -> {
            baseTaskMap.put(baseTask.getName(), baseTask);
            baseTaskMap.putAll(getBaseTask(depth + 1, baseTask.subTasks()));
        });
        return baseTaskMap;
    }

    private void doAppendTask(int depth, Map<String, TaskInfo> taskMap, Map<String, BaseTask> baseTaskMap, Map<String, Map<String, TaskInfo>> taskNameToSubTasks) {
        if (MapUtils.isEmpty(taskMap)) {
            return;
        }

        // 设置baseTask
        taskMap.values().forEach(taskInfo -> taskInfo.setTask(baseTaskMap.get(DAGWalkHelper.getInstance().getBaseTaskName(taskInfo))));
        if (depth >= SystemConfig.getTaskMaxDepth()) {
            return;
        }

        // 设置子Task
        Map<String, TaskInfo> subTaskMap = Maps.newHashMap();
        taskMap.forEach((taskName, taskInfo) -> {
            Optional.ofNullable(taskNameToSubTasks.get(taskName)).ifPresent(taskInfo::setChildren);
            subTaskMap.putAll(taskInfo.getChildren());
        });

        doAppendTask(depth + 1, subTaskMap, baseTaskMap, taskNameToSubTasks);
    }

    private void appendTaskRelation(int depth, Map<String, TaskInfo> taskInfoMap) {
        if (depth >= SystemConfig.getTaskMaxDepth() || MapUtils.isEmpty(taskInfoMap)) {
            return;
        }

        taskInfoMap.values().stream()
                .filter(task -> MapUtils.isNotEmpty(task.getChildren()))
                .forEach(task -> task.getChildren().values().forEach(childrenTask -> childrenTask.setParent(task)));

        TaskInfoMaker.getMaker().appendNextAndDependencyTask(taskInfoMap);

        taskInfoMap.values().stream()
                .filter(task -> MapUtils.isNotEmpty(task.getChildren()))
                .forEach(task -> appendTaskRelation(depth + 1, task.getChildren()));
    }

    public TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        return getTaskInfo(executionId, taskName, null);
    }

    public TaskInfo getTaskInfoWithAllSubTask(String executionId, String taskName) {
        return getTaskInfo(executionId, taskName, TASK_FIELD_PREFIX);
    }

    /**
     * 返回该任务的父任务及与其在同一个遍历中的其他子任务 如: foreachTaskA有子任务
     * foreachTaskA_0-A0 foreachTaskA_0-A1 foreachTaskA_0-A2 foreachTaskA_1-A0 foreachTaskA_1-A1 foreachTaskA_1-A2
     * 若传入taskName为foreachTaskA_0-A1 返回 foreachTaskA与子任务 foreachTaskA_0-A0 foreachTaskA_0-A1 foreachTaskA_0-A2
     */
    public TaskInfo getParentTaskInfoWithSibling(String executionId, String taskName) {
        log.info("getParentTaskInfoWithSibling executionId:{} taskName:{}", executionId, taskName);

        List<String> chainNames = DAGWalkHelper.getInstance().taskInfoNamesCurrentChain(taskName);
        if (chainNames.size() < 2) {
            log.info("getParentTaskInfoWithSibling ancestor task, executionId:{}, taskName:{}", executionId, taskName);
            return null;
        }

        String taskRootName = DAGWalkHelper.getInstance().getRootName(taskName);
        // redis中taskInfo对应的field为 "#" + routeName + "-" + baseTaskName
        // 同一组taskInfo field前缀为 "#" + routeName + "-"
        // - 在lua正则表达式中为特殊字符表示 匹配前一字符0次或多次 需要加%转义
        String subTaskPrefix = (TASK_FIELD_PREFIX + taskRootName + ReservedConstant.TASK_NAME_CONNECTOR)
                .replaceAll(ReservedConstant.TASK_NAME_CONNECTOR, "%" + ReservedConstant.TASK_NAME_CONNECTOR);
        return getTaskInfo(executionId, chainNames.get(chainNames.size() - 2), subTaskPrefix);
    }

    @SuppressWarnings("unchecked")
    private TaskInfo getTaskInfo(String executionId, String taskName, String subTaskPrefix) {
        log.info("getTaskInfo executionId:{} taskName:{} subTaskPrefix:{}", executionId, taskName, subTaskPrefix);

        boolean needSubTasks = StringUtils.isNotEmpty(subTaskPrefix);
        List<String> chainNames = DAGWalkHelper.getInstance().taskInfoNamesCurrentChain(taskName);
        boolean dagDescriberTaskInfoInSameKey = chainNames.size() < 2;

        List<String> keys = Lists.newArrayList();
        List<String> argv = Lists.newArrayList();
        keys.add(buildDagInfoRedisKey(executionId));
        argv.add(DAG_DESCRIBER); // 获取dag描述文件内容 构造TaskInfo.baskTask 及 task间依赖关系
        if (!dagDescriberTaskInfoInSameKey) {
            keys.add(buildSubTaskRedisKey(executionId, chainNames.get(chainNames.size() - 2)));
            argv.add(ReservedConstant.PLACEHOLDER);
        }
        argv.add(buildTaskNameRedisField(taskName)); // 获取taskInfo
        if (needSubTasks) {
            keys.add(buildSubTaskRedisKey(executionId, taskName));
            argv.add(ReservedConstant.PLACEHOLDER); // 获取子任务TaskInfo
            argv.add(ReservedConstant.KEY_PREFIX);
            argv.add(subTaskPrefix);
        }
        List<List<byte[]>> ret = (List<List<byte[]>>) redisClient.eval(RedisScriptManager.dagInfoGetByFieldScript(), executionId, keys, argv);

        List<byte[]> contents = ret.stream()
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .toList();
        checkDAGInfoLength(executionId, contents);

        // dag描述符
        DAG dag = DagStorageSerializer.deserialize(ret.get(0).get(0), DAG.class);
        Map<String, BaseTask> baseTaskMap = getBaseTask(1, Optional.ofNullable(dag).map(DAG::getTasks).orElse(null));
        // taskInfo
        byte[] rawTaskInfo = dagDescriberTaskInfoInSameKey ? ret.get(0).get(1) : ret.get(1).get(0);
        if (rawTaskInfo == null || rawTaskInfo.length == 0) {
            throw new SerializationException(StorageErrorCode.SERIALIZATION_FAIL.getCode(), "storage can not get taskInfo:" + taskName);
        }
        TaskInfo taskInfo = DagStorageSerializer.deserialize(rawTaskInfo, TaskInfo.class);
        taskInfo.setTask(baseTaskMap.get(DAGWalkHelper.getInstance().getBaseTaskName(taskInfo)));
        // subTaskInfo
        if (needSubTasks) {
            Map<String, TaskInfo> subTaskInfos = new LinkedHashMap<>();
            ret.get(ret.size() - 1).stream()
                    .filter(Objects::nonNull)
                    .map(rawSubTask -> DagStorageSerializer.deserialize(rawSubTask, TaskInfo.class))
                    .peek(it -> it.setTask(baseTaskMap.get(DAGWalkHelper.getInstance().getBaseTaskName(it))))
                    .forEach(it -> subTaskInfos.put(it.getName(), it));
            taskInfo.setChildren(subTaskInfos);
        }
        appendTaskRelation(1, ImmutableMap.of(taskInfo.getName(), taskInfo));

        return taskInfo;
    }

    @SuppressWarnings("unchecked")
    public DAG getDAGDescriptor(String executionId) {
        log.info("getDAGDescriptor executionId:{}", executionId);

        List<String> keys = Lists.newArrayList(buildDagInfoRedisKey(executionId));
        List<String> argv = Lists.newArrayList(DAG_DESCRIBER);
        List<List<byte[]>> ret = (List<List<byte[]>>) redisClient.eval(RedisScriptManager.dagInfoGetByFieldScript(), executionId, keys, argv);

        return DagStorageSerializer.deserialize(ret.get(0).get(0), DAG.class);
    }

    public void updateDAGDescriptor(String executionId, DAG dag) {
        if (dag == null) {
            return;
        }

        log.info("updateDAGDescriptor executionId:{}", executionId);
        List<String> keys = Lists.newArrayList();
        List<String> argv = Lists.newArrayList();

        argv.add(String.valueOf(getUnfinishedStatusReserveTimeInSecond(executionId)));

        String descriptor = DagStorageSerializer.serializeToString(dag);
        String descriptorKey = buildDagDescriptorRedisKey(descriptor);
        keys.add(descriptorKey);
        argv.add(ReservedConstant.PLACEHOLDER);
        argv.add(descriptor);

        Map<String, Object> dagInfo = ImmutableMap.of(DAG_DESCRIBER, descriptorKey);
        keys.add(buildDagInfoRedisKey(executionId));
        argv.add(ReservedConstant.PLACEHOLDER);
        argv.addAll(DagStorageSerializer.serializeHashToList(dagInfo));

        redisClient.eval(RedisScriptManager.dagInfoSetScript(), executionId, keys, argv);
    }

    public void delDagInfo(String executionId) {
        delDagInfo(executionId, getFinishStatusReserveTimeInSecond(executionId));
    }

    public void delDagInfo(String executionId, int expireTimeInSecond) {
        try {
            if (expireTimeInSecond < 0) {
                return;
            }

            log.info("delDagInfo executionId:{} expireTime:{}", executionId, expireTimeInSecond);
            redisClient.eval(RedisScriptManager.getRedisExpire(),
                    executionId,
                    Lists.newArrayList(buildDagInfoRedisKey(executionId), buildTaskNameToSubTaskRedisKey(executionId)),
                    Lists.newArrayList(String.valueOf(expireTimeInSecond)));
        } catch (Exception e) {
            log.warn("delDagInfo fails, executionId:{}, expireTimeInSecond:{}", executionId, expireTimeInSecond, e);
            throw e;
        }
    }

    public void updateDagInfo(String executionId, DAGInfo dagInfo) {
        if (dagInfo == null) {
            return;
        }

        try {
            log.info("updateDagInfo executionId:{}", executionId);
            DAGInfo dagInfoClone = DAGInfo.cloneToSave(dagInfo);

            List<String> keys = Lists.newArrayList();
            List<String> argv = Lists.newArrayList();
            serializeDagInfo(executionId, dagInfoClone, keys, argv);

            redisClient.eval(RedisScriptManager.dagInfoSetScript(), executionId, keys, argv);
        } catch (Exception e) {
            log.warn("updateDagInfo fails, executionId:{}", executionId, e);
            throw e;
        }
    }

    private void serializeDagInfo(String executionId, DAGInfo dagInfoClone, List<String> keys, List<String> argv) {
        argv.add(String.valueOf(getUnfinishedStatusReserveTimeInSecond(executionId)));

        String descriptorKey = null;
        if (dagInfoClone.getDag() != null) {
            String descriptor = DagStorageSerializer.serializeToString(dagInfoClone.getDag());
            descriptorKey = buildDagDescriptorRedisKey(descriptor);
            keys.add(descriptorKey);
            argv.add(ReservedConstant.PLACEHOLDER);
            argv.add(descriptor);
        }

        Map<String, Object> dagInfo = Maps.newHashMap();
        dagInfo.put(EXECUTION_ID, dagInfoClone.getExecutionId());
        Optional.ofNullable(descriptorKey).ifPresent(dagDescriptor -> dagInfo.put(DAG_DESCRIBER, dagDescriptor));
        Optional.ofNullable(dagInfoClone.getDagInvokeMsg()).ifPresent(dagInvokeMsg -> dagInfo.put(DAG_INVOKE_MSG, dagInvokeMsg));
        Optional.ofNullable(dagInfoClone.getDagStatus()).ifPresent(dagStatus -> dagInfo.put(DAG_STATUS, dagStatus));
        dagInfoClone.getTasks().forEach((taskName, taskInfo) -> dagInfo.put(buildTaskNameRedisField(taskName), taskInfo));

        Map<String, Map<String, TaskInfo>> taskNameToSubTasks = getSubTasks(1, dagInfoClone.getTasks());

        // DAGInfo hash内容
        keys.add(buildDagInfoRedisKey(executionId));
        argv.add(ReservedConstant.PLACEHOLDER);
        argv.addAll(DagStorageSerializer.serializeHashToList(dagInfo));

        serializeSubTasks(executionId, keys, argv, taskNameToSubTasks);
    }

    private void serializeSubTasks(String executionId, List<String> keys, List<String> argv, Map<String, Map<String, TaskInfo>> taskNameToSubTasks) {
        if (MapUtils.isEmpty(taskNameToSubTasks)) {
            return;
        }

        // 子任务信息
        Map<String, String> taskNameToSubTaskRedisKey = Maps.newHashMap();
        taskNameToSubTasks.forEach((taskName, subTasks) -> {
            if (MapUtils.isEmpty(subTasks)) {
                return;
            }

            String subTaskRedisKey = buildSubTaskRedisKey(executionId, taskName);
            keys.add(subTaskRedisKey);
            argv.add(ReservedConstant.PLACEHOLDER);
            argv.addAll(DagStorageSerializer.serializeHashToList(subTasks));
            taskNameToSubTaskRedisKey.put(taskName, subTaskRedisKey);
        });

        // 任务与其子任务映射
        if (MapUtils.isNotEmpty(taskNameToSubTaskRedisKey)) {
            keys.add(buildTaskNameToSubTaskRedisKey(executionId));
            argv.add(ReservedConstant.PLACEHOLDER);
            taskNameToSubTaskRedisKey.forEach((taskName, subTaskRedisKey) -> {
                argv.add(taskName);
                argv.add(subTaskRedisKey);
            });
        }
    }

    private Map<String, Map<String, TaskInfo>> getSubTasks(int depth, Map<String, TaskInfo> tasks) {
        Map<String, Map<String, TaskInfo>> taskNameToSubTasks = Maps.newHashMap();
        tasks.forEach((taskName, taskInfo) -> {
            if (StringUtils.isEmpty(taskName) || taskInfo == null || MapUtils.isEmpty(taskInfo.getChildren())) {
                return;
            }

            Map<String, TaskInfo> subTasks = taskInfo.getChildren().entrySet().stream()
                    .collect(Collectors.toMap(entry -> buildTaskNameRedisField(entry.getKey()), Map.Entry::getValue));
            taskNameToSubTasks.put(taskName, subTasks);
            if (depth < SystemConfig.getTaskMaxDepth()) {
                taskNameToSubTasks.putAll(getSubTasks(depth + 1, taskInfo.getChildren()));
                taskInfo.setChildren(new LinkedHashMap<>());
            }
        });
        return taskNameToSubTasks;
    }

    public void saveTaskInfos(String executionId, Set<TaskInfo> taskInfos) {
        try {
            log.info("saveTaskInfos executionId:{}", executionId);
            if (CollectionUtils.isEmpty(taskInfos)) {
                log.info("saveTaskInfos taskInfos empty, executionId:{}, taskInfos:{}", executionId, taskInfos);
                return;
            }

            List<String> keys = Lists.newArrayList();
            List<String> argv = Lists.newArrayList();
            serializeTaskInfos(executionId, taskInfos, keys, argv);

            redisClient.eval(RedisScriptManager.dagInfoSetScript(), executionId, keys, argv);
        } catch (Exception e) {
            log.warn("saveTaskInfos fails, executionId:{}", executionId, e);
            throw e;
        }
    }

    private void serializeTaskInfos(String executionId, Set<TaskInfo> taskInfos, List<String> keys, List<String> argv) {
        Map<String, TaskInfo> clonedTaskInfos = taskInfos.stream().
                map(TaskInfo::cloneToSave)
                .collect(Collectors.toMap(TaskInfo::getName, taskInfo -> taskInfo));

        Map<String, Map<String, TaskInfo>> taskNameToSubTasks = getSubTasks(1, clonedTaskInfos);
        Map<String, TaskInfo> ancestorTaskMap = Maps.newHashMap();
        clonedTaskInfos.values().forEach(taskInfo -> {
            String taskName = taskInfo.getName();
            List<String> chainNames = DAGWalkHelper.getInstance().taskInfoNamesCurrentChain(taskName);
            if (chainNames.size() < 2) {
                ancestorTaskMap.put(buildTaskNameRedisField(taskName), taskInfo);
                return;
            }
            Map<String, TaskInfo> subTaskMap = taskNameToSubTasks.computeIfAbsent(chainNames.get(chainNames.size() - 2), key -> Maps.newHashMap());
            subTaskMap.put(buildTaskNameRedisField(taskName), taskInfo);
        });

        argv.add(String.valueOf(getUnfinishedStatusReserveTimeInSecond(executionId)));

        if (MapUtils.isNotEmpty(ancestorTaskMap)) {
            keys.add(buildDagInfoRedisKey(executionId));
            argv.add(ReservedConstant.PLACEHOLDER);
            argv.addAll(DagStorageSerializer.serializeHashToList(ancestorTaskMap));
        }

        serializeSubTasks(executionId, keys, argv, taskNameToSubTasks);
    }

    private String buildDagInfoRedisKey(String executionId) {
        return DAGRedisPrefix.PREFIX_DAG_INFO.getValue() + executionId;
    }

    private String buildDagDescriptorRedisKey(String descriptor) {
        String md5 = DigestUtils.md5Hex(descriptor);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMM");
        String time = dateFormat.format(new Date());
        return DAGRedisPrefix.PREFIX_DAG_DESCRIPTOR.getValue() + time + "_" + md5;
    }

    private String buildTaskNameToSubTaskRedisKey(String executionId) {
        return DAGRedisPrefix.PREFIX_SUB_TASK_MAPPING.getValue() + executionId;
    }

    private String buildSubTaskRedisKey(String executionId, String parentTaskName) {
        return DAGRedisPrefix.PREFIX_SUB_TASK.getValue() + executionId + "_" + parentTaskName;
    }

    private String buildTaskNameRedisField(String taskName) {
        return TASK_FIELD_PREFIX + taskName;
    }
}
