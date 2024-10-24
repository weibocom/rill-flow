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

package com.weibo.rill.flow.service.strategies;

import com.weibo.rill.flow.olympicene.core.model.dag.DAG;

public interface DAGProcessStrategy {
    /**
     * 存储数据时处理 dag
     * @param dag 工作流对象
     * @return 处理后的工作流对象
     */
    DAG transformDAGProperties(DAG dag);

    /**
     * 取回数据时处理 descriptor
     * @param descriptor 工作流描述
     * @return 处理后的工作流描述
     */
    String transformDescriptor(String descriptor);
}
