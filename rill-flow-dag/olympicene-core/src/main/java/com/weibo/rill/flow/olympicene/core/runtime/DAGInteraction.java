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

package com.weibo.rill.flow.olympicene.core.runtime;

import com.weibo.rill.flow.olympicene.core.model.DAGSettings;
import com.weibo.rill.flow.olympicene.core.model.NotifyInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGResult;

import java.util.Map;

/**
 * DAG执行控制方法
 * Created by xilong on 2021/4/8.
 */
public interface DAGInteraction {

    /**
     * 提交需执行的DAG任务
     */
    void submit(String executionId, String taskName, DAG dag, Map<String, Object> data, DAGSettings settings, NotifyInfo notifyInfo);

    /**
     * 完成task后调用接口
     */
    void finish(String executionId, DAGSettings settings, Map<String, Object> data, NotifyInfo notifyInfo);

    /**
     * 唤醒suspense任务
     */
    void wakeup(String executionId, Map<String, Object> data, NotifyInfo notifyInfo);

    /**
     * 重新执行任务
     */
    void redo(String executionId, Map<String, Object> data, NotifyInfo notifyInfo);

    /**
     * 执行的DAG任务并返回执行结果
     */
    DAGResult run(String executionId, DAG dag, Map<String, Object> data, DAGSettings settings, NotifyInfo notifyInfo);
}
