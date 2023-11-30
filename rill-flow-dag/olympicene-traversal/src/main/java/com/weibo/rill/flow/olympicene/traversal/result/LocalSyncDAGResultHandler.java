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

package com.weibo.rill.flow.olympicene.traversal.result;

import com.weibo.rill.flow.olympicene.core.model.dag.DAGResult;
import com.weibo.rill.flow.olympicene.core.result.DAGResultHandler;
import com.weibo.rill.flow.olympicene.traversal.constant.TraversalErrorCode;
import com.weibo.rill.flow.olympicene.traversal.exception.DAGTraversalException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Slf4j
public class LocalSyncDAGResultHandler implements DAGResultHandler {
    private final Map<String, CountDownLatch> needHandleResult = new ConcurrentHashMap<>();
    private final Map<String, DAGResult> executionIdToDAGResult = new ConcurrentHashMap<>();

    @Override
    public void initEnv(String executionId) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        needHandleResult.put(executionId, countDownLatch);
    }

    @Override
    public boolean updateDAGResult(String executionId, DAGResult dagResult) {
        try {
            Optional.ofNullable(needHandleResult.get(executionId)).ifPresent(handle -> {
                executionIdToDAGResult.put(executionId, dagResult);
                handle.countDown();
            });
            return true;
        } catch (Exception e) {
            log.warn("updateDAGResult fails due to executionId:{} errorMsg:{}", executionId, e.getMessage());
            return false;
        }
    }

    @Override
    public DAGResult getDAGResult(String executionId, long timeoutInMillisecond) {
        try {
            CountDownLatch countDownLatch = needHandleResult.get(executionId);
            if (!countDownLatch.await(timeoutInMillisecond, TimeUnit.MILLISECONDS)) {
                log.info("getDAGResult cannot get result in configured time, executionId:{}, timeout:{}", executionId, timeoutInMillisecond);
            }
            needHandleResult.remove(executionId);
            DAGResult dagResult = executionIdToDAGResult.remove(executionId);
            if (dagResult == null) {
                throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(),
                        "cannot get dagResult in " + timeoutInMillisecond + " milliseconds");
            }
            return dagResult;
        } catch (DAGTraversalException e) {
            throw e;
        } catch (Exception e) {
            throw new DAGTraversalException(TraversalErrorCode.DAG_ILLEGAL_STATE.getCode(), "getDAGResult fails", e.getCause());
        } finally {
            needHandleResult.remove(executionId);
            executionIdToDAGResult.remove(executionId);
        }
    }
}
