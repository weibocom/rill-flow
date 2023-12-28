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

package com.weibo.rill.flow.trigger.triggers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.facade.OlympiceneFacade;
import com.weibo.rill.flow.trigger.util.TriggerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Service("cron_trigger")
@Slf4j
@RestController
@EnableScheduling
public class ScheduleTrigger implements Trigger {
    @Autowired
    @Qualifier("dagDefaultStorageRedisClient")
    RedisClient redisClient;
    @Autowired
    private OlympiceneFacade olympiceneFacade;

    private final ThreadPoolTaskScheduler taskScheduler;
    private final Map<String, ScheduledFuture<?>> scheduledTaskMap;
    private final Map<String, JSONObject> taskInfoMap;

    private static final String SCHEDULED_TASK_ID_KEY = "scheduled_task_id";
    private static final String SCHEDULED_TASKS = "scheduled_tasks";

    public ScheduleTrigger() {
        this.taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(500);
        taskScheduler.initialize();
        scheduledTaskMap = new ConcurrentHashMap<>();
        taskInfoMap = new ConcurrentHashMap<>();
    }

    public void scheduleTask(String taskId, String cronTime, Runnable task) throws Exception {
        try {
            if (scheduledTaskMap.containsKey(taskId)) {
                throw new Exception("task id exists");
            }
            // fixme: 如果项目要被用于分布式环境，需要考虑多机的分布式并发问题
            synchronized (scheduledTaskMap) {
                if (scheduledTaskMap.containsKey(taskId)) {
                    scheduledTaskMap.get(taskId);
                    return;
                }
                ScheduledFuture<?> scheduledTask = taskScheduler.schedule(task, new CronTrigger(cronTime));
                scheduledTaskMap.put(taskId, scheduledTask);
            }
        } catch (Exception e) {
            log.warn("scheduleTask error, taskId: {}, cronTime: {}", taskId, cronTime, e);
            throw e;
        }
    }

    public boolean cancelTask(String taskId) {
        try {
            synchronized (scheduledTaskMap) {
                ScheduledFuture<?> scheduledFuture = scheduledTaskMap.get(taskId);
                if (scheduledFuture == null || scheduledFuture.isCancelled()) {
                    return false;
                }
                scheduledFuture.cancel(false);
                return cancelScheduler(taskId);
            }
        } catch (Exception e) {
            log.warn("cancelTask error, taskId: {}", taskId, e);
            return false;
        }
    }

    private boolean cancelScheduler(String taskId) {
        try {
            redisClient.hdel(SCHEDULED_TASKS, taskId);
            return cancelTask(taskId);
        } catch (Exception e) {
            log.warn("cancel scheduler error, task_id: {}", taskId, e);
            return false;
        }
    }

    @Override
    public JSONObject addTriggerTask(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject body) {
        return addCronTrigger(uid, descriptorId, callback, resourceCheck, body, null);
    }

    @Override
    public void initTriggerTasks() {
        Map<String, String> cronTasks = redisClient.hgetAll(SCHEDULED_TASKS);
        for (Map.Entry<String, String> taskEntry: cronTasks.entrySet()) {
            String taskId = taskEntry.getKey();
            String taskDetail = taskEntry.getValue();
            JSONObject taskDetailObject = JSON.parseObject(taskDetail);
            JSONObject context = taskDetailObject.getJSONObject("context");
            String callback = taskDetailObject.getString("callback");
            String resourceCheck = taskDetailObject.getString("resource_check");
            String descriptorId = taskDetailObject.getString("descriptor_id");
            Long uid = taskDetailObject.getLong("uid");
            addCronTrigger(uid, descriptorId, callback, resourceCheck, context, taskId);
        }
    }

    @Override
    public boolean cancelTriggerTask(String taskId) {
        return cancelTask(taskId);
    }

    @Override
    public Map<String, JSONObject> getTriggerTasks() {
        return taskInfoMap;
    }

    public JSONObject addCronTrigger(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject body, String taskId) {
        JSONObject context = body.getJSONObject("context");
        String cron = body.getString("cron");
        Runnable runnable = () -> olympiceneFacade.submit(uid, descriptorId, callback, resourceCheck, context, "add_scheduler.json");
        // generate task id from redis
        if (StringUtils.isEmpty(taskId)) {
            taskId = String.valueOf(redisClient.incr(SCHEDULED_TASK_ID_KEY));
            insertCronDetailToRedis(uid, descriptorId, callback, resourceCheck, taskId, context, cron);
        }
        taskInfoMap.put(taskId, body);
        try {
            scheduleTask(taskId, cron, runnable);
            return new JSONObject(Map.of("code", 0, "data", Map.of("task_id", taskId)));
        } catch (Exception e) {
            log.warn("add scheduler error, task_id: {}, cron: {}", taskId, cron, e);
            cancelScheduler(taskId);
            return new JSONObject(Map.of("code", -1, "err_msg", e.getMessage()));
        }
    }

    private void insertCronDetailToRedis(Long uid, String descriptorId, String callback, String resourceCheck,
                                         String taskId, JSONObject context, String cron) {
        // insert detail infos into redis
        JSONObject jsonDetails = TriggerUtil.buildCommonDetail(uid, descriptorId, callback, resourceCheck);
        jsonDetails.put("cron", cron);
        jsonDetails.put("context", context);
        redisClient.hset(SCHEDULED_TASKS, taskId, jsonDetails.toJSONString());
    }

}
