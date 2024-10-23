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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

@Slf4j
@Component("dagProcessStrategyContext")
public class DAGProcessStrategyContext {
    @Resource
    private Map<String, DAGProcessStrategy> strategies;

    public static final String DEFAULT_STRATEGY = "defaultDAGProcessStrategy";
    public static final String CUSTOM_STRATEGY = "customDAGProcessStrategy";

    public DAG onStorage(DAG dag, String strategyName) {
        DAGProcessStrategy strategy = strategies.get(strategyName);
        if (strategy == null) {
            log.warn("strategy {} not found on storage", strategyName);
            strategy = strategies.get(DEFAULT_STRATEGY);
        }
        return strategy.onStorage(dag);
    }

    public String onRetrieval(String descriptor, String strategyName) {
        DAGProcessStrategy strategy = strategies.get(strategyName);
        if (strategy == null) {
            log.warn("strategy {} not found on retrieval", strategyName);
            strategy = strategies.get(DEFAULT_STRATEGY);
        }
        return strategy.onRetrieval(descriptor);
    }
}
