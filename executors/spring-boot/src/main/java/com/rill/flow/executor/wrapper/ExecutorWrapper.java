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

package com.rill.flow.executor.wrapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * @author yansheng3
 */
@Service
@Slf4j
public class ExecutorWrapper {

    private final ExecutorService executor;

    @Autowired
    private TaskFinishCallback callback;

    public ExecutorWrapper() {
        this.executor = Executors.newFixedThreadPool(10);
    }

    public ExecutorWrapper(ExecutorService executorService) {
        this.executor = executorService;
    }

    public Map<String, Object> execute(ExecutorContext context, Function<ExecutorContext, Map<String, Object>> function) {

        boolean isAsyncMode = Optional.ofNullable(context).map(ExecutorContext::getMode).map(it -> it.equals("async")).orElse(false);
        if (isAsyncMode) {
            executor.submit(() -> asyncExecute(context, function));
            return Map.of("result_type", "SUCCESS");
        } else {
            return doExecute(context, function);
        }

    }

    private void asyncExecute(ExecutorContext context, Function<ExecutorContext, Map<String, Object>> function) {
        CallbackData.CallbackDataBuilder callbackBuilder = CallbackData.builder().url(context.getCallbackUrl());
        try {
            Map<String, Object> executorResponse = doExecute(context, function);
            executorResponse.put("result_type", "SUCCESS");
            callbackBuilder.result(executorResponse);
        } catch (Exception e) {
            callbackBuilder.result(Map.of("result_type", "FAILED", "error_msg", e.getMessage()));
            log.error("execute is failed. error_msg:{}", e.getMessage());
        } finally {
            callback.onCompletion(callbackBuilder.build());
        }
    }

    private Map<String, Object> doExecute(ExecutorContext context, Function<ExecutorContext, Map<String, Object>> function) {
        return function.apply(context);
    }
}
