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

package com.weibo.rill.flow.olympicene.core.constant;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.olympicene.core.model.task.ExecutionResult;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;


public class SystemConfig {
    /**
     * 假设foreach类型任务包含100个子任务
     * 任务深度为3时 可能会产生1w个子任务
     *  foreach  100        第一层
     *      foreach 100     第二层
     *          function    第三层
     * 任务深度为4时 可能会产生100w个子任务 任务数量太大
     */
    private static volatile int taskMaxDepth = 3;

    public static int getTaskMaxDepth() {
        return taskMaxDepth;
    }

    public static void setTaskMaxDepth(int taskMaxDepth) {
        SystemConfig.taskMaxDepth = taskMaxDepth;
    }

    /**
     * 执行重试次数
     */
    private static volatile int traversalRetryTimes = 1;

    private static volatile int timerRetryTimes = 1;

    public static int getTraversalRetryTimes() {
        return traversalRetryTimes;
    }

    public static void setTraversalRetryTimes(int traversalRetryTimes) {
        SystemConfig.traversalRetryTimes = traversalRetryTimes;
    }

    public static int getTimerRetryTimes() {
        return timerRetryTimes;
    }

    public static void setTimerRetryTimes(int timerRetryTimes) {
        SystemConfig.timerRetryTimes = timerRetryTimes;
    }

    /**
     * 系统运行插件
     */
    public static final List<BiConsumer<Runnable, Map<String, Object>>> TRAVERSAL_CUSTOMIZED_PLUGINS = Lists.newCopyOnWriteArrayList();

    public static final List<BiConsumer<Runnable, Map<String, Object>>> NOTIFY_CUSTOMIZED_PLUGINS = Lists.newCopyOnWriteArrayList();

    public static final List<BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult>> TASK_RUN_CUSTOMIZED_PLUGINS = Lists.newCopyOnWriteArrayList();

    public static final List<BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult>> TASK_FINISH_CUSTOMIZED_PLUGINS = Lists.newCopyOnWriteArrayList();

    public static final List<BiFunction<Supplier<ExecutionResult>, Map<String, Object>, ExecutionResult>> DAG_FINISH_CUSTOMIZED_PLUGINS = Lists.newCopyOnWriteArrayList();

    private SystemConfig() {

    }
}
