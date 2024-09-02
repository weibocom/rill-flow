package com.weibo.rill.flow.executors.controller;

import com.weibo.rill.flow.executors.model.SseDispatchParams;
import com.weibo.rill.flow.executors.service.SseExecutorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.Resource;
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
    private SseExecutorService sseExecutorService;

    @ApiOperation(value = "派发sse任务")
    @PostMapping(value = "dispatch.json")
    public Map<String, Object> dispatch(@ApiParam(value = "工作流执行ID") @RequestParam(value = "execution_id") String executionId,
                                        @ApiParam(value = "工作流任务名称") @RequestParam(value = "task_name") String taskName,
                                        @ApiParam(value = "任务详细信息") @RequestBody(required = false) SseDispatchParams params) {
        UUID uuid = UUID.randomUUID();
        String uuidStr = uuid.toString();
        sseThreadPoolExecutor.submit(() -> sseExecutorService.callSSETask(executionId, taskName, uuidStr, params));
        return Map.of("uuid", uuidStr);
    }

    @ApiOperation(value = "查询 sse 任务执行结果")
    @GetMapping(value = "get_result.json", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getResult(@ApiParam(value = "sse 任务唯一 id") @RequestParam(value = "uuid") String uuid) {
        SseEmitter emitter = new SseEmitter();
        sseThreadPoolExecutor.execute(() -> sseExecutorService.getSSEData(uuid, emitter));
        return emitter;
    }
}
