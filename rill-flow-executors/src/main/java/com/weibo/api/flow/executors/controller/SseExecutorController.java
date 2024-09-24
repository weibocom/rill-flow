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

package com.weibo.api.flow.executors.controller;

import com.alibaba.fastjson.JSONObject;
import com.weibo.api.flow.executors.model.constant.SseConstant;
import com.weibo.api.flow.executors.model.event.SseEvent;
import com.weibo.api.flow.executors.model.params.SseDispatchParams;
import com.weibo.api.flow.executors.service.SseExecutorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
@Api(tags = {"SSE协议执行器相关接口"})
@RequestMapping("/2/flow/executor/sse")
@Slf4j
public class SseExecutorController {
    @Value("${rill.flow.server.host:http://i.faas.api.weibo.com}")
    private String rillFlowServerHost;

    @Resource
    private ThreadPoolExecutor sseThreadPoolExecutor;
    @Resource
    private SseExecutorService sseExecutorService;
    @Resource
    private ApplicationEventPublisher applicationEventPublisher;

    @ApiOperation(value = "派发sse任务")
    @PostMapping(value = "execute.json")
    public Map<String, Object> execute(@ApiParam(value = "工作流执行ID") @RequestParam(value = "execution_id") String executionId,
                                       @ApiParam(value = "工作流任务名称") @RequestParam(value = "task_name") String taskName,
                                       @ApiParam(value = "任务详细信息") @RequestBody SseDispatchParams params) {
        sseThreadPoolExecutor.submit(() -> sseExecutorService.callSSETask(executionId, taskName, params));
        return Map.of("data", "success");
    }

    @ApiOperation(value = "查询 sse 任务执行结果")
    @GetMapping(value = "get_result.sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getResult(@ApiParam(value = "工作流执行ID") @RequestParam(value = "execution_id") String executionId,
                                @ApiParam(value = "工作流任务名称") @RequestParam(value = "task_name") String taskName,
                                @ApiParam(value = "静默模式") @RequestParam(value = "quiet", required = false, defaultValue = "false") boolean quiet,
                                @ApiParam(value = "rill-flow host") @RequestParam(value = "rill_flow_host", required = false) String rillFlowHost) {
        if (StringUtils.isEmpty(rillFlowHost)) {
            rillFlowHost = rillFlowServerHost;
        }
        String finalRillFlowHost = rillFlowHost;
        SseEmitter emitter = new SseEmitter(SseConstant.SSE_EMITTER_TIMEOUT);
        sseThreadPoolExecutor.submit(() -> {
            try {
                sseExecutorService.getSseTaskResult(finalRillFlowHost, executionId, null, taskName, emitter, quiet);
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("get_sse_result_finished")
                        .executionId(executionId)
                        .taskName(taskName)
                        .emitter(emitter)
                        .quiet(quiet)
                        .completed(true)
                        .build()));
            } catch (Exception e) {
                applicationEventPublisher.publishEvent(new SseEvent(SseEvent.SseOperation.builder()
                        .event("get_sse_result_error")
                        .executionId(executionId)
                        .taskName(taskName)
                        .emitter(emitter)
                        .quiet(quiet)
                        .exception(e)
                        .completed(true)
                        .build()));
            }
        });
        return emitter;
    }

    @ApiOperation(value = "提交执行并启动查询")
    @PostMapping(value = "submit_for_task_result.sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter submitForTaskResult(@ApiParam(value = "DAG 图描述符 id") @RequestParam(value = "descriptor_id") String descriptorId,
                                          @ApiParam(value = "sse 任务名称") @RequestParam(value = "task_name") String taskName,
                                          @ApiParam(value = "静默模式") @RequestParam(value = "quiet", required = false, defaultValue = "false") boolean quiet,
                                          @ApiParam(value = "rill-flow host") @RequestParam(value = "rill_flow_host", required = false) String rillFlowHost,
                                          @ApiParam(value = "图初始上下文") @RequestBody JSONObject context) {
        if (StringUtils.isEmpty(rillFlowHost)) {
            rillFlowHost = rillFlowServerHost;
        }
        return sseExecutorService.submitForResult(rillFlowHost, descriptorId, taskName, context, quiet);
    }
}
