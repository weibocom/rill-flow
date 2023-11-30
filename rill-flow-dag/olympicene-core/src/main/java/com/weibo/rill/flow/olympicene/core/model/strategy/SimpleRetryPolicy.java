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

package com.weibo.rill.flow.olympicene.core.model.strategy;

import com.weibo.rill.flow.interfaces.model.strategy.Retry;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInvokeMsg;
import com.weibo.rill.flow.interfaces.model.task.TaskStatus;

import java.util.List;
import java.util.Optional;


public class SimpleRetryPolicy implements RetryPolicy {
    @Override
    public boolean needRetry(RetryContext context) {
        if (context.getTaskStatus() != TaskStatus.FAILED) {
            return false;
        }

        int invokeTimes = Optional.ofNullable(context.getTaskInfo())
                .map(TaskInfo::getTaskInvokeMsg)
                .map(TaskInvokeMsg::getInvokeTimeInfos)
                .map(List::size)
                .orElse(1);
        int maxRetryTimes = Optional.ofNullable(context.getRetryConfig()).map(Retry::getMaxRetryTimes).orElse(0);

        return invokeTimes <= maxRetryTimes;
    }

    @Override
    public int calculateRetryInterval(RetryContext context) {
        int intervalInSeconds = Optional.ofNullable(context.getRetryConfig())
                .map(Retry::getIntervalInSeconds).filter(interval -> interval >= 0).orElse(0);
        double multiplier = Optional.ofNullable(context.getRetryConfig())
                .map(Retry::getMultiplier).filter(it -> it > 0).orElse(1D);
        int invokeTimes = Optional.ofNullable(context.getTaskInfo())
                .map(TaskInfo::getTaskInvokeMsg)
                .map(TaskInvokeMsg::getInvokeTimeInfos)
                .map(List::size)
                .orElse(1);

        double currentMultiplier = Math.pow(multiplier, invokeTimes);
        return (int) (intervalInSeconds * currentMultiplier);
    }
}
