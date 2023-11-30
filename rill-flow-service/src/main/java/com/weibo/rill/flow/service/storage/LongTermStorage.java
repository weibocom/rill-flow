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
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo;
import com.weibo.rill.flow.service.storage.dao.DAGPikaDAO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Map;


@Slf4j
public class LongTermStorage {
    private final BizDConfs bizDConfs;
    private final DAGPikaDAO dagPikaDAO;

    public LongTermStorage(BizDConfs bizDConfs, Map<String, RedisClient> clientIdToRedisClient) {
        this.bizDConfs = bizDConfs;
        this.dagPikaDAO = new DAGPikaDAO(false, clientIdToRedisClient, bizDConfs);
    }

    public void storeDAGInfoAndContext(DAGCallbackInfo callbackInfo) {
        if (callbackInfo == null) {
            return;
        }

        String executionId = callbackInfo.getExecutionId();
        try {
            boolean needStore = storageExist(executionId);
            log.info("storeDAGInfoAndContext executionId:{} needStore:{}", executionId, needStore);
            if (!needStore) {
                return;
            }
            dagPikaDAO.saveDAGInfo(executionId, callbackInfo.getDagInfo());
            dagPikaDAO.saveContext(executionId, callbackInfo.getContext());
        } catch (Exception e) {
            log.warn("storeDAGInfoAndContext fails, executionId:{}", executionId, e);
        }
    }

    public DAGInfo getDAGInfo(String executionId) {
        boolean storageExist = storageExist(executionId);
        log.info("getDAGInfo executionId:{} storageExist:{}", executionId, storageExist);
        if (!storageExist) {
            return null;
        }
        return dagPikaDAO.getDAGInfo(executionId);
    }

    public DAGInfo getBasicDAGInfo(String executionId) {
        boolean storageExist = storageExist(executionId);
        log.info("getBasicDAGInfo executionId:{} storageExist:{}", executionId, storageExist);
        if (!storageExist) {
            return null;
        }
        return dagPikaDAO.getBasicDAGInfo(executionId);
    }

    public TaskInfo getBasicTaskInfo(String executionId, String taskName) {
        boolean storageExist = storageExist(executionId);
        log.info("getBasicTaskInfo executionId:{} storageExist:{}", executionId, storageExist);
        if (!storageExist) {
            return null;
        }
        return dagPikaDAO.getBasicTaskInfo(executionId, taskName);
    }

    public TaskInfo getTaskInfo(String executionId, String taskName, String subGroupIndex) {
        boolean storageExist = storageExist(executionId);
        log.info("getTaskInfo executionId:{} storageExist:{}", executionId, storageExist);
        if (!storageExist) {
            return null;
        }
        return dagPikaDAO.getTaskInfo(executionId, taskName, subGroupIndex);
    }

    public Map<String, Object> getContext(String executionId) {
        boolean storageExist = storageExist(executionId);
        log.info("getContext executionId:{} storageExist:{}", executionId, storageExist);
        if (!storageExist) {
            return Maps.newHashMap();
        }
        return dagPikaDAO.getContext(executionId, false);
    }

    public Map<String, Object> getContext(String executionId, Collection<String> fields) {
        boolean storageExist = storageExist(executionId);
        log.info("getContext executionId:{} storageExist:{}", executionId, storageExist);
        return dagPikaDAO.getContext(executionId, fields);
    }

    private boolean storageExist(String executionId) {
        if (StringUtils.isBlank(executionId)) {
            return false;
        }

        try {
            String businessId = ExecutionIdUtil.getBusinessId(executionId);
            return bizDConfs.getLongTermStorageBusinessIdToClientId().containsKey(businessId);
        } catch (Exception e) {
            log.warn("storageExist fails, executionId:{}, errorMsg:{}", executionId, e.getMessage());
            return false;
        }
    }
}
