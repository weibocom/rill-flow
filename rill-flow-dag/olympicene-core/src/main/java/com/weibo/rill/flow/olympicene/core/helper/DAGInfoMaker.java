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

package com.weibo.rill.flow.olympicene.core.helper;

import com.weibo.rill.flow.olympicene.core.constant.CoreErrorCode;
import com.weibo.rill.flow.interfaces.model.exception.DAGException;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;

import java.util.Optional;

/**
 * 通过DAG构造DAGInfo，需要将next指针够着成DAG结构
 * 支持动态生成新结点的功能
 */
public class DAGInfoMaker {
    private String executionId;
    private DAG dag;
    private DAGInvokeMsg dagInvokeMsg;
    private DAGStatus dagStatus;

    public DAGInfoMaker executionId(String executionId) {
        this.executionId = executionId;
        return this;
    }

    public DAGInfoMaker dagInvokeMsg(DAGInvokeMsg dagInvokeMsg) {
        this.dagInvokeMsg = dagInvokeMsg;
        return this;
    }

    public DAGInfoMaker dagStatus(DAGStatus status) {
        this.dagStatus = status;
        return this;
    }

    public DAGInfoMaker dag(DAG dag) {
        this.dag = dag;
        return this;
    }

    public DAGInfo make() {
        DAGInfo result = new DAGInfo();
        result.setExecutionId(executionId);
        result.setDag(Optional.ofNullable(dag).orElseThrow(() -> new DAGException(CoreErrorCode.DAG_STATE_NONSUPPORT.getCode(), "dag null")));
        result.setDagInvokeMsg(dagInvokeMsg);
        result.setDagStatus(Optional.ofNullable(dagStatus).orElse(DAGStatus.NOT_STARTED));
        result.setTasks(TaskInfoMaker.getMaker().makeTaskInfos(dag.getTasks()));
        return result;
    }
}
