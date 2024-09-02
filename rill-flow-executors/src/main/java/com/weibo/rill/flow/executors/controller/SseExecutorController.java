package com.weibo.rill.flow.executors.controller;

import com.weibo.rill.flow.executors.model.SseDispatchParams;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
@Api(tags = {"SSE协议执行器相关接口"})
@RequestMapping("/flow/sse")
public class SseExecutorController {
    @Resource
    private ThreadPoolExecutor sseThreadPoolExecutor;
    @Resource
    private RestTemplate restTemplate;

    @ApiOperation(value = "派发sse任务")
    @PostMapping(value = "dispatch.json")
    public Map<String, Object> dispatch(@ApiParam(value = "工作流执行ID") @RequestParam(value = "execution_id") String executionId,
                                        @ApiParam(value = "工作流执行ID") @RequestParam(value = "task_name") String taskName,
                                        @ApiParam(value = "任务详细信息") @RequestBody(required = false) SseDispatchParams params) {
        sseThreadPoolExecutor.submit(() -> {

        });
        return new HashMap<>();
    }
}
