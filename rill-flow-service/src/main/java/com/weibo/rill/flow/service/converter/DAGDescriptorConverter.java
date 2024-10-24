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

package com.weibo.rill.flow.service.converter;

import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorPO;
import com.weibo.rill.flow.olympicene.core.model.dag.DescriptorVO;

public interface DAGDescriptorConverter {
    DAG convertDescriptorVOToDAG(DescriptorVO descriptorVO);
    DAG convertDescriptorPOToDAG(DescriptorPO descriptorPO);
    DescriptorVO convertDAGToDescriptorVO(DAG dag);
    DescriptorPO convertDAGToDescriptorPO(DAG dag);
}
