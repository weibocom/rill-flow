package com.weibo.rill.flow.executors.service;

import com.weibo.rill.flow.executors.model.SseDispatchParams;
import com.weibo.rill.flow.executors.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class SseExecutorService {
    @Resource
    private RestTemplate rillFlowSSEHttpTemplate;
    @Resource
    private RedisClient sseExecutorRedisClient;

    private static final String REDIS_KEY_PREFIX = "sse-executor-";
    private static final String SSE_MARK = "data:";
    private static final int RPOP_COUNT = 50;

    public void getSSEData(String uuid, SseEmitter emitter) {
        try {
            while (true) {
                boolean isComplete = getDataFromRedis(uuid, emitter);
                if (isComplete) {
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
            emitter.complete();
        } catch (Exception e) {
            emitter.completeWithError(e);
        }
    }

    private boolean getDataFromRedis(String uuid, SseEmitter emitter) throws IOException {
        List<String> dataList = sseExecutorRedisClient.rpop(REDIS_KEY_PREFIX + uuid, RPOP_COUNT);
        if (dataList == null) {
            return false;
        }
        for (String word : dataList) {
            emitter.send(word);
            if ((uuid + "-completed").equals(word)) {
                return true;
            }
        }
        return false;
    }

    public void callSSETask(String executionId, String taskName, String id, SseDispatchParams params) {
        try {
            HttpMethod method = HttpMethod.resolve(params.getRequestType());
            if (method == null) {
                method = HttpMethod.POST;
            }
            rillFlowSSEHttpTemplate.execute(params.getUrl(), method, request -> {},
                    response -> handleSseTaskResponse(executionId, taskName, id, response), params.getInput());
        } catch (Exception e) {
            log.warn("Error while executing sse, execution_id: {}, task_name: {}: ", executionId, taskName, e);
        } finally {
            sseExecutorRedisClient.lpush(REDIS_KEY_PREFIX + id, id + "-completed");
        }
    }

    @Nullable
    private String handleSseTaskResponse(String executionId, String taskName, String id, ClientHttpResponse response) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.getBody()));
        String data;
        try {
            while ((data = bufferedReader.readLine()) != null) {
                if (!data.startsWith(SSE_MARK)) {
                    continue;
                }
                data = data.substring(SSE_MARK.length());
                long redisResult = sseExecutorRedisClient.lpush(REDIS_KEY_PREFIX + id, data);
                log.info("got sse data: {}, id: {}, redis result: {}", data, id, redisResult);
            }
        } catch (IOException e) {
            log.warn("Error while reading, execution_id: {}, task_name: {}, id: {}, data: ", executionId, taskName, id, e);
        }
        return null;
    }
}
