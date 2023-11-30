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

import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.util.IpUtils;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.redis.lock.impl.RedisDistributedLocker;
import com.weibo.rill.flow.olympicene.storage.save.impl.RedisStorageProcedure;
import com.weibo.rill.flow.service.storage.dao.DAGPikaDAO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.Set;
import java.util.UUID;


@Slf4j
public class RuntimeSwapStorage {
    private static final String DAG_INFO_LOCK_KEY_PREFIX = "swap_lock_dag_info_";
    private static final String CONTEXT_LOCK_KEY_PREFIX = "swap_lock_context_";

    private final DAGPikaDAO dagPikaDAO;
    private final RedisStorageProcedure storageProcedure;

    public RuntimeSwapStorage(RedisClient redisClient, Map<String, RedisClient> clientIdToRedisClient, BizDConfs bizDConfs) {
        String instanceId = IpUtils.getLocalIpv4Address() + UUID.randomUUID().toString().replace("-", "");
        this.storageProcedure = new RedisStorageProcedure(instanceId, new RedisDistributedLocker(redisClient));
        this.dagPikaDAO = new DAGPikaDAO(true, clientIdToRedisClient, bizDConfs);
    }

    public void saveDAGInfo(String executionId, DAGInfo dagInfo) {
        if (dagInfo == null) {
            log.info("saveDAGInfo dagInfo null, executionId:{}", executionId);
            return;
        }
        storageProcedure.lockAndRun(buildDagInfoLockName(executionId),
                () -> dagPikaDAO.updateDAGInfo(executionId, dagInfo));
    }

    public void saveTaskInfos(String executionId, Set<TaskInfo> taskInfos) {
        if (CollectionUtils.isEmpty(taskInfos)) {
            log.info("saveTaskInfos taskInfos empty, executionId:{}", executionId);
            return;
        }
        storageProcedure.lockAndRun(buildDagInfoLockName(executionId),
                () -> dagPikaDAO.updateTaskInfos(executionId, taskInfos));
    }

    public DAGInfo getDAGInfo(String executionId) {
        return dagPikaDAO.getDAGInfo(executionId);
    }

    public void clearDAGInfo(String executionId) {
        storageProcedure.lockAndRun(buildDagInfoLockName(executionId),
                () -> dagPikaDAO.clearDAGInfo(executionId));
    }

    public void updateDAGDescriptor(String executionId, DAG dag) {
        if (dag == null) {
            log.info("updateDAGDescriptor dag null, executionId:{}", executionId);
            return;
        }
        storageProcedure.lockAndRun(buildDagInfoLockName(executionId),
                () -> dagPikaDAO.updateDAGDescriptor(executionId, dag));
    }

    public void updateContext(String executionId, Map<String, Object> context) {
        if (MapUtils.isEmpty(context)) {
            log.info("updateContext context empty, executionId:{}", executionId);
            return;
        }
        storageProcedure.lockAndRun(buildContextLockName(executionId),
                () -> dagPikaDAO.updateContext(executionId, context));
    }

    public Map<String, Object> getTotalContext(String executionId) {
        return dagPikaDAO.getTotalContext(executionId);
    }

    public void clearContext(String executionId) {
        storageProcedure.lockAndRun(buildContextLockName(executionId),
                () -> dagPikaDAO.clearContext(executionId));
    }

    private String buildDagInfoLockName(String executionId) {
        return DAG_INFO_LOCK_KEY_PREFIX + executionId;
    }

    private String buildContextLockName(String executionId) {
        return CONTEXT_LOCK_KEY_PREFIX + executionId;
    }
}
