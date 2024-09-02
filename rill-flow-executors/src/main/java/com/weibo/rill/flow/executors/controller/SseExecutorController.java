package com.weibo.rill.flow.executors.controller;

import com.weibo.rill.flow.executors.model.SseDispatchParams;
import com.weibo.rill.flow.executors.redis.RedisClient;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
@Api(tags = {"SSE协议执行器相关接口"})
@RequestMapping("/flow/sse")
@Slf4j
public class SseExecutorController {
    @Resource
    private ThreadPoolExecutor sseThreadPoolExecutor;
    @Resource
    private RestTemplate rillFlowSSEHttpTemplate;
    @Resource
    private RedisClient sseExecutorRedisClient;

    private static final String REDIS_KEY_PREFIX = "sse-executor-";
    private static final String SSE_MARK = "data:";

    @ApiOperation(value = "派发sse任务")
    @PostMapping(value = "dispatch.json")
    public Map<String, Object> dispatch(@ApiParam(value = "工作流执行ID") @RequestParam(value = "execution_id") String executionId,
                                        @ApiParam(value = "工作流执行ID") @RequestParam(value = "task_name") String taskName,
                                        @ApiParam(value = "任务详细信息") @RequestBody(required = false) SseDispatchParams params) {
        UUID uuid = UUID.randomUUID();
        String uuidStr = uuid.toString();
        sseThreadPoolExecutor.submit(() -> callSSETask(executionId, taskName, uuidStr, params));
        return Map.of("uuid", uuidStr);
    }

    private void callSSETask(String executionId, String taskName, String id, SseDispatchParams params) {
        try {
            HttpMethod method = HttpMethod.resolve(params.getRequestType());
            if (method == null) {
                method = HttpMethod.POST;
            }
            rillFlowSSEHttpTemplate.execute(params.getUrl(), method, request -> {}, response -> {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.getBody()));
                String data;
                try {
                    while ((data = bufferedReader.readLine()) != null) {
                        if (data.startsWith(SSE_MARK)) {
                            data = data.substring(data.indexOf(SSE_MARK) + SSE_MARK.length());
                        } else {
                            continue;
                        }
                        long redisResult = sseExecutorRedisClient.lpush(REDIS_KEY_PREFIX + id, data.substring(SSE_MARK.length()));
                        log.info("got sse data: {}, redis result: {}", data, redisResult);
                    }
                } catch (IOException e) {
                    log.warn("Error while reading, execution_id: {}, task_name: {}, data: ", executionId, taskName, e);
                }
                return null;
            }, params.getInput());
        } catch (Exception e) {
            log.warn("Error while executing sse, execution_id: {}, task_name: {}: ", executionId, taskName, e);
        } finally {
            sseExecutorRedisClient.lpush(REDIS_KEY_PREFIX + id, id + "-completed");
        }
    }
}
