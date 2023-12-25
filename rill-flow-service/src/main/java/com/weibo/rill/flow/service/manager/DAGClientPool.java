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

package com.weibo.rill.flow.service.manager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.service.configuration.BeanConfig;
import com.weibo.rill.flow.service.configuration.BeanGenerator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;


@Slf4j
@Getter
@Component
public class DAGClientPool implements ApplicationContextAware {
    private static final String RUNTIME_STORAGE_CLIENT_BEAN_PREFIX = "runtimeStorage_";
    private static final String RUNTIME_STORAGE_CLIENT_EXECUTOR_BEAN_PREFIX = "runtimeStorage_exec_";
    private static final String LONG_TERM_STORAGE_CLIENT_BEAN_PREFIX = "longTermStorage_";
    private static final String CUSTOMIZED_STORAGE_CLIENT_BEAN_PREFIX = "customizedStorage_";
    private static final String HTTP_CLIENT_REST_TEMPLATE_BEAN_PREFIX = "restTemplate_";

    private static final Object RUNTIME_LOCK = new Object();
    private static final Object LONG_TERM_LOCK = new Object();
    private static final Object CUSTOMIZED_LOCK = new Object();
    private static final Object HTTP_LOCK = new Object();

    private final Map<String, RedisClient> runtimeStorageClientIdToRedisClient = Maps.newConcurrentMap();
    private final Map<String, ExecutorService> runtimeExecutorClientMapping = Maps.newConcurrentMap();
    private final Map<String, RedisClient> longTermStorageClientIdToRedisClient = Maps.newConcurrentMap();
    private final Map<String, RedisClient> customizedStorageClientIdToRedisClient = Maps.newConcurrentMap();
    private final Map<String, RestTemplate> httpClientIdToRestTemplate = Maps.newConcurrentMap();

    private ApplicationContext applicationContext;
    
    @Autowired
    private List<BeanGenerator<?>> beanGenerators;
    
    @Autowired
    @Qualifier("clientPoolExecutor")
    private ExecutorService clientPoolExecutor;

    public void updateRuntimeRedisClientMap(Map<String, BeanConfig> runtimeStorageConfig) {
        synchronized (RUNTIME_LOCK) {
            updateClientMap(runtimeStorageConfig, runtimeStorageClientIdToRedisClient,
                    RedisClient.class, RUNTIME_STORAGE_CLIENT_BEAN_PREFIX);
            updateClientMap(runtimeStorageConfig,
                    runtimeExecutorClientMapping,
                    ExecutorService.class,
                    RUNTIME_STORAGE_CLIENT_EXECUTOR_BEAN_PREFIX
            );
        }
    }

    public void updateLongTermRedisClientMap(Map<String, BeanConfig> longTermStorageConfig) {
        synchronized (LONG_TERM_LOCK) {
            updateClientMap(longTermStorageConfig, longTermStorageClientIdToRedisClient,
                    RedisClient.class, LONG_TERM_STORAGE_CLIENT_BEAN_PREFIX);
        }
    }

    public void updateCustomizedRedisClientMap(Map<String, BeanConfig> customizedStorageConfig) {
        synchronized (CUSTOMIZED_LOCK) {
            updateClientMap(customizedStorageConfig, customizedStorageClientIdToRedisClient,
                    RedisClient.class, CUSTOMIZED_STORAGE_CLIENT_BEAN_PREFIX);
        }
    }

    public void updateClientIdToRestTemplate(Map<String, BeanConfig> httpConfig) {
        synchronized (HTTP_LOCK) {
            updateClientMap(httpConfig, httpClientIdToRestTemplate, RestTemplate.class, HTTP_CLIENT_REST_TEMPLATE_BEAN_PREFIX);
        }
    }

    private <T> void updateClientMap(Map<String, BeanConfig> configMap, Map<String, ? super T> clientMap,
                                     Class<T> clientType, String beanNamePrefix) {
        if (MapUtils.isEmpty(configMap)) {
            log.info("updateClientMap config map empty beanNamePrefix:{}", beanNamePrefix);
            return;
        }

        BeanGenerator<T> beanGenerator = getGenerator(clientType);
        List<Callable<Void>> beanActions = Lists.newArrayList();
        configMap.forEach((name, config) ->
                beanActions.add(() -> {
                    try {
                        String beanName = beanNamePrefix + name;
                        if (clientMap.containsKey(name)) {
                            log.info("updateClientMap skip, already exist beanName:{} beanNamePrefix:{}", beanName, beanNamePrefix);
                            return null;
                        }

                        long startTime = System.currentTimeMillis();
                        log.info("updateClientMap start to create new instance beanName:{} config:{}", beanName, config);
                        T client = beanGenerator.newInstance(config);
                        log.info("updateClientMap create new instance success beanName:{} config:{} cost:{}", beanName, config, System.currentTimeMillis() - startTime);
                        clientMap.put(name, client);
                        registerBean(beanName, client);
                    } catch (Exception e) {
                        log.warn("updateClientMap fails, name:{}, config:{}, beanNamePrefix:{}", name, config, beanNamePrefix, e);
                    }
                    return null;
                }));

        try {
            clientPoolExecutor.invokeAll(beanActions);
        } catch (Exception e) {
            log.warn("updateClientMap fails, beanNamePrefix{}", beanNamePrefix, e);
            Thread.currentThread().interrupt();
            throw new TaskException(BizError.ERROR_PROCESS_FAIL.getCode(), String.format("can not init beans prefix: %s", beanNamePrefix));
        }
    }

    private void registerBean(String beanName, Object beanObject) {
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getAutowireCapableBeanFactory();
        beanFactory.registerSingleton(beanName, beanObject);
    }

    @SuppressWarnings("unchecked")
    private <T> BeanGenerator<T> getGenerator(Class<T> targetBeanType) {
        return (BeanGenerator<T>) beanGenerators.stream()
                .filter(it -> it.accept(targetBeanType))
                .findFirst()
                .orElseThrow(() -> new TaskException(BizError.ERROR_UNSUPPORTED, "can not find suitable beanGenerator"));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
