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
public class SampleExecutor implements Function<ExecutorContext,Map<String,Object>> {

    @Override
    public Map<String, Object> apply(ExecutorContext executorContext) {
        Map<String, Object> requestBody = executorContext.getBody();
        if (requestBody.isEmpty()) {
            throw new RuntimeException("request body is empty");
        }
        // your business here
        requestBody.put("executor_tag", "executor");
        return requestBody;
    }
}
