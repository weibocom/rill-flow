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
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.function.ResourceCheckConfig;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.impl.model.FlowUser;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.context.DAGContextInitializer;
import com.weibo.rill.flow.service.facade.OlympiceneFacade;
import com.weibo.rill.flow.service.statistic.ProfileRecordService;
import com.weibo.rill.flow.service.util.DescriptorIdUtil;
import com.weibo.rill.flow.trigger.util.TriggerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

@Slf4j
@Service("kafka_trigger")
public class KafkaTrigger implements Trigger {

    @Autowired
    private ProfileRecordService profileRecordService;

    @Autowired
    private OlympiceneFacade olympiceneFacade;

    @Autowired
    private DAGContextInitializer dagContextInitializer;

    @Autowired
    @Qualifier("dagDefaultStorageRedisClient")
    RedisClient redisClient;

    @Value("${kafka.trigger.thread.pool.size:500}")
    private int kafkaTriggerThreadPoolSize;
    private static final Map<String, JSONObject> taskInfos = new HashMap<>();
    private static ThreadPoolExecutor threadPoolExecutor;

    @PostConstruct
    public void kafkaTrigger() {
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(kafkaTriggerThreadPoolSize);
    }

    private Properties createKafkaProperties(String servers, String groupId) {
        Properties kafkaTriggerProperties = new Properties();
        kafkaTriggerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        kafkaTriggerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaTriggerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaTriggerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaTriggerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaTriggerProperties;
    }

    private static final String KAFKA_TRIGGER_TOPICS = "kafka_trigger_topics";

    @Override
    public JSONObject addTriggerTask(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject body) {
        return addTrigger(uid, descriptorId, callback, resourceCheck, body, false);
    }

    public JSONObject addTrigger(Long uid, String descriptorId, String callback, String resourceCheck, JSONObject body, boolean isInit) {
        String topic = body.getString("topic");
        String groupId = body.getString("group_id");
        String kafkaServer = body.getString("kafka_server");
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(descriptorId) || StringUtils.isBlank(kafkaServer) || StringUtils.isBlank(groupId)) {
            log.warn("kafka trigger add trigger error, topic: {}, descriptor_id: {}, server: {}, group_id: {}",
                    topic, descriptorId, kafkaServer, groupId);
            throw new TaskException(BizError.ERROR_MISSING_PARAMETER, "kafka trigger add trigger error");
        }

        log.info("kafka trigger add task, topic: {}, descriptor_id: {}, server: {}, group_id: {}",
                topic, descriptorId, kafkaServer, groupId);

        JSONObject jsonDetails = TriggerUtil.buildCommonDetail(uid, descriptorId, callback, resourceCheck);
        jsonDetails.put("group_id", groupId);
        jsonDetails.put("kafka_server", kafkaServer);
        String taskKey = topic + "#" + descriptorId;
        if (taskInfos.containsKey(taskKey)) {
            return new JSONObject(Map.of("code", -1, "message", "task has already existed"));
        }
        if (!isInit) {
            // 1. insert into redis and put to table
            redisClient.hset(KAFKA_TRIGGER_TOPICS, topic + "#" + descriptorId, jsonDetails.toJSONString());
        }
        // 2. create consumer
        Properties properties = createKafkaProperties(kafkaServer, groupId);
        createConsumer(topic, uid, descriptorId, callback, resourceCheck, properties);
        taskInfos.put(taskKey, jsonDetails);
        return new JSONObject(Map.of("code", 0));
    }

    @Override
    public void initTriggerTasks() {
        Map<String, String> triggerTopics = redisClient.hgetAll(KAFKA_TRIGGER_TOPICS);
        for (Map.Entry<String, String> entry : triggerTopics.entrySet()) {
            String key = entry.getKey();
            String[] keyInfos = key.split("#");
            if (keyInfos.length < 2) {
                continue;
            }
            String topic = keyInfos[0];
            String descriptorId = keyInfos[1];
            JSONObject taskDetail = JSON.parseObject(entry.getValue());
            taskDetail.put("topic", topic);
            addTrigger(taskDetail.getLong("uid"), descriptorId, taskDetail.getString("callback"),
                    taskDetail.getString("resource_check"), taskDetail, true);
        }

    }

    @Override
    public boolean cancelTriggerTask(String taskId) {
        if (!taskInfos.containsKey(taskId)) {
            return false;
        }
        redisClient.hdel(KAFKA_TRIGGER_TOPICS, taskId);
        taskInfos.remove(taskId);
        return true;
    }

    @Override
    public Map<String, JSONObject> getTriggerTasks() {
        return taskInfos;
    }

    private void createConsumer(String topic, Long uid, String descriptorId, String callback, String resourceCheck, Properties properties) {
        String taskKey = topic + "#" + descriptorId;
        log.info("kafka trigger create consumer, topic: {}, descriptor_id: {}, server: {}, group_id: {}",
                topic, descriptorId, properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        try {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            threadPoolExecutor.submit(() -> {
                consumer.subscribe(Collections.singletonList(topic));
                while (true) {
                    if (taskInfos.get(taskKey) == null) {
                        consumer.close();
                        log.info("task canceled: {}", taskKey);
                        break;
                    }
                    consumeRecords(topic, uid, descriptorId, callback, resourceCheck, consumer);
                }
            });
            log.info("kafka trigger create consumer success, topic: {}, descriptor_id: {}", topic, descriptorId);
        } catch (Exception e) {
            log.warn("kafka trigger create consumer error, topic: {}, descriptor_id: {}", topic, descriptorId, e);
        }
    }

    private void consumeRecords(String topic, Long uid, String descriptorId, String callback, String resourceCheck, KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            try {
                String message = record.value();

                Supplier<Map<String, Object>> submitActions = () -> {
                    log.info("kafka trigger consume, topic: {}, descriptor_id: {}, message: {}", topic, descriptorId, message);
                    JSONObject context = JSON.parseObject(message);
                    ResourceCheckConfig resourceCheckConfig = JSON.parseObject(resourceCheck, ResourceCheckConfig.class);
                    String businessId = DescriptorIdUtil.changeDescriptorIdToBusinessId(descriptorId);
                    Map<String, Object> contextMap = dagContextInitializer.newSubmitContextBuilder(businessId).withData(context).withIdentity(descriptorId).build();

                    Map<String, Object> result = olympiceneFacade.submit(new FlowUser(uid), descriptorId, contextMap, callback, resourceCheckConfig);
                    log.info("kafka trigger submit success, topic: {}, descriptor_id: {}, result: {}", topic, descriptorId, result);
                    return result;
                };
                profileRecordService.runNotifyAndRecordProfile("kafka trigger", descriptorId, submitActions);
            } catch (Exception e) {
                log.warn("kafka trigger consume error, topic: {}, descriptor_id: {}", topic, descriptorId, e);
            }
        }
    }

}
