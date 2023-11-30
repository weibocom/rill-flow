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

package com.weibo.rill.flow.olympicene.traversal.helper;

import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;


public class PluginHelper {
    public static Runnable pluginInvokeChain(Runnable basicActions, Map<String, Object> params, List<BiConsumer<Runnable, Map<String, Object>>> plugins) {
        Runnable runnable = basicActions;
        if (CollectionUtils.isNotEmpty(plugins)) {
            for (int i = plugins.size() - 1; i >= 0; i--) {
                BiConsumer<Runnable, Map<String, Object>> plugin = plugins.get(i);
                Runnable previousRunnable = runnable;
                runnable = () -> plugin.accept(previousRunnable, params);
            }
        }
        return runnable;
    }

    public static Supplier<ExecutionResult> pluginInvokeChain(Supplier<ExecutionResult> basicActions, Map<String, Object> params,
                                                              List<BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult>> plugins) {
        Supplier<ExecutionResult> supplier = basicActions;
        if (CollectionUtils.isNotEmpty(plugins)) {
            for (int i = plugins.size() - 1; i >= 0; i--) {
                BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult> plugin = plugins.get(i);
                Supplier<ExecutionResult> previousSupplier = supplier;
                supplier = () -> plugin.apply(previousSupplier, params);
            }
        }
        return supplier;
    }

    private PluginHelper() {

    }
}
