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

package com.rill.flow.executor.executor;

import com.rill.flow.executor.wrapper.ExecutorContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Function;

/**
 * @author yansheng3
 */
@Service
@Slf4j
public class SampleExecutor implements Function<ExecutorContext, Map<String, Object>> {

    @Override
    public Map<String, Object> apply(ExecutorContext executorContext) {
        Map<String, Object> requestBody = executorContext.getBody();
        if (requestBody == null) {
            throw new RuntimeException("request body is empty");
        }
        // your business here
        requestBody.put("executor_tag", "executor");
        return requestBody;
    }
}
