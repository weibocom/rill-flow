package com.weibo.rill.flow.executors.controller;

import com.alibaba.fastjson.JSONObject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@Api(tags = {"SSE协议执行器相关接口"})
@RequestMapping("/flow/sse")
public class SseExecutorController {
    @ApiOperation(value = "派发sse任务")
    @RequestMapping(value = "dispatch.json", method = RequestMethod.POST)
    public Map<String, Object> dispatch(@ApiParam(value = "工作流执行ID") @RequestParam(value = "execution_id") String execution_id,
                                        @ApiParam(value = "任务详细信息") @RequestBody(required = false) JSONObject data) {
        return new HashMap<>();
    }
}
