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

package com.weibo.rill.flow.service.storage;

import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.constant.ReservedConstant;
import com.weibo.rill.flow.olympicene.core.helper.DAGWalkHelper;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.storage.constant.StorageErrorCode;
import com.weibo.rill.flow.olympicene.storage.exception.StorageException;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDeserializeService;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGRedisStorage;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.storage.dao.ContextRedisDAO;
import com.weibo.rill.flow.service.storage.dao.DAGInfoRedisDAO;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


@Slf4j
public class RuntimeStorage implements DAGInfoStorage, DAGContextStorage {
    private final DAGRedisStorage runtimeRedisStorage;
    private final RuntimeSwapStorage runtimeSwapStorage;
    private final BizDConfs bizDConfs;

    public RuntimeStorage(RedisClient redisClient, Map<String, RedisClient> clientIdToRedisClient, BizDConfs bizDConfs,
                          DAGInfoDeserializeService dagInfoDeserializeService, SwitcherManager switcherManagerImpl) {
        this.bizDConfs = bizDConfs;

        DAGInfoRedisDAO dagInfoRedisDAO = new DAGInfoRedisDAO(redisClient, bizDConfs, dagInfoDeserializeService, switcherManagerImpl);
        ContextRedisDAO contextRedisDAO = new ContextRedisDAO(redisClient, bizDConfs, switcherManagerImpl);
        this.runtimeRedisStorage = new DAGRedisStorage(dagInfoRedisDAO, contextRedisDAO);

        this.runtimeSwapStorage = new RuntimeSwapStorage(redisClient, clientIdToRedisClient, bizDConfs);
    }

    @Override
    public void saveDAGInfo(String executionId, DAGInfo dagInfo) {
        if (dagInfo == null) {
            return;
        }

        Runnable redisOperation = () -> runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
        Runnable swapOperation = () -> runtimeSwapStorage.saveDAGInfo(executionId, dagInfo);
        setAction(executionId, redisOperation, swapOperation);
    }

    @Override
    public void saveTaskInfos(String executionId, Set<TaskInfo> taskInfos) {
        if (CollectionUtils.isEmpty(taskInfos)) {
            log.info("saveTaskInfos taskInfos empty, executionId:{}, taskInfos:{}", executionId, taskInfos);
            return;
        }

        Runnable redisOperation = () -> runtimeRedisStorage.saveTaskInfos(executionId, taskInfos);
        Runnable swapOperation = () -> runtimeSwapStorage.saveTaskInfos(executionId, taskInfos);
        setAction(executionId, redisOperation, swapOperation);
    }

    @Override
    public DAGInfo getDAGInfo(String executionId) {
        Supplier<DAGInfo> redisOperation = () -> runtimeRedisStorage.getDAGInfo(executionId);
        Function<DAGInfo, Boolean> isValueAcquired = Objects::nonNull;
        Supplier<DAGInfo> swapOperation = () -> {
            DAGInfo dagInfo = runtimeSwapStorage.getDAGInfo(executionId);
            if (dagInfo != null) {
                runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
            }
            return dagInfo;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public DAGInfo getBasicDAGInfo(String executionId) {
        Supplier<DAGInfo> redisOperation = () -> runtimeRedisStorage.getBasicDAGInfo(executionId);
        Function<DAGInfo, Boolean> isValueAcquired = Objects::nonNull;
        Supplier<DAGInfo> swapOperation = () -> {
            DAGInfo dagInfo = runtimeSwapStorage.getDAGInfo(executionId);
            if (dagInfo != null) {
                runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
                Optional.ofNullable(dagInfo.getTasks()).map(Map::values)
                        .ifPresent(taskInfos -> taskInfos.forEach(taskInfo -> taskInfo.setChildren(new LinkedHashMap<>())));
            }
            return dagInfo;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        Supplier<TaskInfo> redisOperation = () -> runtimeRedisStorage.getBasicTaskInfo(executionId, taskName);
        Function<TaskInfo, Boolean> isValueAcquired = Objects::nonNull;
        Supplier<TaskInfo> swapOperation = () -> {
            TaskInfo taskInfo = null;
            DAGInfo dagInfo = runtimeSwapStorage.getDAGInfo(executionId);
            if (dagInfo != null) {
                runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
                taskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskName);
                taskInfo.setChildren(new LinkedHashMap<>());
            }
            return taskInfo;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public TaskInfo getTaskInfo(String executionId, String taskName) {
        Supplier<TaskInfo> redisOperation = () -> runtimeRedisStorage.getTaskInfo(executionId, taskName);
        Function<TaskInfo, Boolean> isValueAcquired = Objects::nonNull;
        Supplier<TaskInfo> swapOperation = () -> {
            TaskInfo taskInfo = null;
            DAGInfo dagInfo = runtimeSwapStorage.getDAGInfo(executionId);
            if (dagInfo != null) {
                runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
                taskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskName);
            }
            return taskInfo;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public TaskInfo getParentTaskInfoWithSibling(String executionId, String taskName) {
        Supplier<TaskInfo> redisOperation = () -> runtimeRedisStorage.getParentTaskInfoWithSibling(executionId, taskName);
        Function<TaskInfo, Boolean> isValueAcquired = Objects::nonNull;
        Supplier<TaskInfo> swapOperation = () -> {
            TaskInfo taskInfo = null;
            DAGInfo dagInfo = runtimeSwapStorage.getDAGInfo(executionId);
            if (dagInfo != null) {
                runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
                TaskInfo currentTaskInfo = DAGWalkHelper.getInstance().getTaskInfoByName(dagInfo, taskName);
                TaskInfo parentTaskInfo = currentTaskInfo.getParent();
                Map<String, TaskInfo> sibling = parentTaskInfo.getChildren().entrySet().stream()
                        .filter(entry -> entry.getValue().getRouteName().equals(currentTaskInfo.getRouteName()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                parentTaskInfo.setChildren(sibling);
                taskInfo = parentTaskInfo;
            }
            return taskInfo;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public void clearDAGInfo(String executionId) {
        Runnable redisOperation = () -> runtimeRedisStorage.clearDAGInfo(executionId);
        Runnable swapOperation = () -> runtimeSwapStorage.clearDAGInfo(executionId);
        setAction(executionId, redisOperation, swapOperation);
    }

    @Override
    public void clearDAGInfo(String executionId, int expireTimeInSecond) {
        Runnable redisOperation = () -> runtimeRedisStorage.clearDAGInfo(executionId, expireTimeInSecond);
        Runnable swapOperation = () -> runtimeSwapStorage.clearDAGInfo(executionId);
        setAction(executionId, redisOperation, swapOperation);
    }

    @Override
    public DAG getDAGDescriptor(String executionId) {
        Supplier<DAG> redisOperation = () -> runtimeRedisStorage.getDAGDescriptor(executionId);
        Function<DAG, Boolean> isValueAcquired = Objects::nonNull;
        Supplier<DAG> swapOperation = () -> {
            DAG dag = null;
            DAGInfo dagInfo = runtimeSwapStorage.getDAGInfo(executionId);
            if (dagInfo != null) {
                runtimeRedisStorage.saveDAGInfo(executionId, dagInfo);
                dag = dagInfo.getDag();
            }
            return dag;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public void updateDAGDescriptor(String executionId, DAG dag) {
        if (dag == null) {
            return;
        }

        Runnable redisOperation = () -> runtimeRedisStorage.updateDAGDescriptor(executionId, dag);
        Runnable swapOperation = () -> runtimeSwapStorage.updateDAGDescriptor(executionId, dag);
        setAction(executionId, redisOperation, swapOperation);
    }

    @Override
    public void updateContext(String executionId, Map<String, Object> context) {
        if (MapUtils.isEmpty(context)) {
            return;
        }

        Runnable redisOperation = () -> runtimeRedisStorage.updateContext(executionId, context);
        Runnable swapOperation = () -> runtimeSwapStorage.updateContext(executionId, context);
        setAction(executionId, redisOperation, swapOperation);
    }

    @Override
    public Map<String, Object> getContext(String executionId) {
        Supplier<Map<String, Object>> redisOperation = () -> runtimeRedisStorage.getContext(executionId);
        Function<Map<String, Object>, Boolean> isValueAcquired = MapUtils::isNotEmpty;
        Supplier<Map<String, Object>> swapOperation = () -> {
            Map<String, Object> ret = Maps.newHashMap();
            Map<String, Object> totalContext = runtimeSwapStorage.getTotalContext(executionId);
            if (MapUtils.isNotEmpty(totalContext)) {
                runtimeRedisStorage.updateContext(executionId, totalContext);
                ret = totalContext.entrySet().stream()
                        .filter(entry -> !entry.getKey().startsWith(ReservedConstant.SUB_CONTEXT_PREFIX))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            return ret;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public Map<String, Object> getContext(String executionId, Collection<String> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return Maps.newHashMap();
        }

        Supplier<Map<String, Object>> redisOperation = () -> runtimeRedisStorage.getContext(executionId, fields);
        Function<Map<String, Object>, Boolean> isValueAcquired = MapUtils::isNotEmpty;
        Supplier<Map<String, Object>> swapOperation = () -> {
            Map<String, Object> ret = Maps.newHashMap();
            Map<String, Object> totalContext = runtimeSwapStorage.getTotalContext(executionId);
            if (MapUtils.isNotEmpty(totalContext)) {
                runtimeRedisStorage.updateContext(executionId, totalContext);
                fields.forEach(field -> Optional.ofNullable(totalContext.get(field))
                        .ifPresent(value -> ret.put(field, value)));
            }
            return ret;
        };
        return getAction(executionId, redisOperation, isValueAcquired, swapOperation);
    }

    @Override
    public void clearContext(String executionId) {
        Runnable redisOperation = () -> runtimeRedisStorage.clearContext(executionId);
        Runnable swapOperation = () -> runtimeSwapStorage.clearContext(executionId);
        setAction(executionId, redisOperation, swapOperation);
    }

    private <T> T getAction(String executionId, Supplier<T> redisOperation, Function<T, Boolean> isValueAcquired, Supplier<T> swapOperation) {
        if (!swapExist(executionId)) {
            return redisOperation.get();
        }

        T redisValue;
        try {
            redisValue = redisOperation.get();
            if (isValueAcquired.apply(redisValue)) {
                return redisValue;
            }
        } catch (Exception e) {
            if (e instanceof StorageException &&
                    ((StorageException) e).getErrorCode() == StorageErrorCode.DAG_LENGTH_LIMITATION.getCode()) {
                throw e;
            }

            T swapValue = getSwapValue(executionId, swapOperation);
            if (isValueAcquired.apply(swapValue)) {
                return swapValue;
            }
            throw e;
        }

        T swapValue = getSwapValue(executionId, swapOperation);
        return isValueAcquired.apply(swapValue) ? swapValue : redisValue;
    }

    private <T> T getSwapValue(String executionId, Supplier<T> swapOperation) {
        try {
            log.info("get value from swap storage executionId:{}", executionId);
            return swapOperation.get();
        } catch (Exception e) {
            log.warn("getSwapValue fails, executionId:{}", executionId, e);
            return null;
        }
    }

    private void setAction(String executionId, Runnable redisOperation, Runnable swapOperation) {
        redisOperation.run();

        if (swapExist(executionId)) {
            try {
                swapOperation.run();
            } catch (Exception e) {
                log.warn("setAction fails, executionId:{}", executionId, e);
            }
        }
    }

    private boolean swapExist(String executionId) {
        try {
            String businessId = ExecutionIdUtil.getBusinessId(executionId);
            return bizDConfs.getSwapBusinessIdToClientId().containsKey(businessId);
        } catch (Exception e) {
            log.warn("swapExist fails, executionId:{}, errorMsg:{}", executionId, e.getMessage());
            return false;
        }
    }
}
