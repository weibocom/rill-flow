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

package com.weibo.rill.flow.configuration;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.olympicene.core.event.Callback;
import com.weibo.rill.flow.olympicene.core.model.task.*;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.ddl.serialize.ObjectMapperFactory;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.storage.redis.lock.impl.RedisDistributedLocker;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDeserializeService;
import com.weibo.rill.flow.olympicene.storage.save.impl.RedisStorageProcedure;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.olympicene.traversal.helper.SameThreadExecutorService;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping;
import com.weibo.rill.flow.service.component.OlympiceneCallback;
import com.weibo.rill.flow.service.component.RuntimeExecutorServiceProxy;
import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.service.decorator.ShareMdcFeatureDecoratorAssembler;
import com.weibo.rill.flow.service.decorator.TaskDecoratingExecutorServiceDecorator;
import com.weibo.rill.flow.service.dispatcher.FunctionTaskDispatcher;
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper;
import com.weibo.rill.flow.service.manager.DAGClientPool;
import com.weibo.rill.flow.service.mapping.JsonValueMapping;
import com.weibo.rill.flow.service.statistic.BusinessTimeChecker;
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic;
import com.weibo.rill.flow.service.storage.LongTermStorage;
import com.weibo.rill.flow.service.storage.RuntimeRedisClients;
import com.weibo.rill.flow.service.storage.RuntimeStorage;
import com.weibo.rill.flow.service.util.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

@Configuration
@AutoConfigureOrder(1)
public class OlympiceneConfiguration {

    @Bean
    public LongTermStorage longTermStorage(
            @Autowired BizDConfs bizDConfs,
            @Autowired DAGClientPool dagClientPool) {
        return new LongTermStorage(bizDConfs, dagClientPool.getLongTermStorageClientIdToRedisClient());
    }

    @Bean
    public Callback<DAGCallbackInfo> dagCallback(
            @Autowired HttpInvokeHelper httpInvokeHelper,
            @Autowired LongTermStorage longTermStorage,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired @Qualifier("callbackExecutor") ExecutorService callbackExecutor,
            @Autowired TenantTaskStatistic tenantTaskStatistic,
            @Autowired SwitcherManager switcherManagerImpl) {
        return new OlympiceneCallback(httpInvokeHelper, inputOutputMapping, longTermStorage, callbackExecutor, tenantTaskStatistic, switcherManagerImpl);
    }

    @Bean
    public DAGDispatcher dagTaskDispatcher(@Autowired FunctionTaskDispatcher functionTaskDispatcher) {
        return functionTaskDispatcher;
    }

    @Bean
    public JSONPathInputOutputMapping inputOutputMapping() {
        return new JsonValueMapping();
    }

    @Bean
    public RuntimeRedisClients runtimeRedisClients(
            @Autowired BizDConfs bizDConfs,
            @Autowired DAGClientPool dagClientPool,
            @Autowired @Qualifier("dagDefaultStorageRedisClient") RedisClient defaultRedisClient) {
        return new RuntimeRedisClients(bizDConfs, dagClientPool.getRuntimeStorageClientIdToRedisClient(), defaultRedisClient);
    }

    @Bean(name = {"dagInfoStorage", "dagContextStorage"})
    public RuntimeStorage runtimeStorage(
            @Autowired BizDConfs bizDConfs,
            @Autowired DAGClientPool dagClientPool,
            @Autowired @Qualifier("runtimeRedisClients") RedisClient redisClient,
            @Autowired DAGInfoDeserializeService dagInfoDeserializeService,
            @Autowired SwitcherManager switcherManagerImpl) {
        return new RuntimeStorage(redisClient, dagClientPool.getRuntimeStorageClientIdToRedisClient(), bizDConfs, dagInfoDeserializeService, switcherManagerImpl);
    }

    @Bean
    public DAGStorageProcedure dagStorageProcedure(
            @Autowired @Qualifier("runtimeRedisClients") RedisClient redisClient) {
        String instanceId = IpUtils.getLocalIpv4Address() + UUID.randomUUID().toString().replace("-", "");
        return new RedisStorageProcedure(instanceId, new RedisDistributedLocker(redisClient));
    }

    @Bean
    public BusinessTimeChecker timeChecker(
            @Autowired @Qualifier("runtimeRedisClients") RedisClient redisClient) {
        return new BusinessTimeChecker(redisClient);
    }


    @Bean(destroyMethod = "shutdown")
    public ExecutorService notifyExecutor(@Autowired BizDConfs bizDConfs,
                                          @Autowired DAGClientPool dagClientPool) {
        return new RuntimeExecutorServiceProxy(
                bizDConfs,
                dagClientPool.getRuntimeExecutorClientMapping(),
                SameThreadExecutorService.INSTANCE
        );
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService traversalExecutor(@Autowired BizDConfs bizDConfs,
                                             @Autowired DAGClientPool dagClientPool) {

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("olympicene-traversal-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(20, 100, 100000,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1000), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        TaskDecoratingExecutorServiceDecorator decorator = new TaskDecoratingExecutorServiceDecorator(threadPoolExecutor);
        decorator.setTaskDecoratorAssemblerList(List.of(new ShareMdcFeatureDecoratorAssembler()));

        return new RuntimeExecutorServiceProxy(
                bizDConfs,
                dagClientPool.getRuntimeExecutorClientMapping(),
                decorator
        );
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService runnerExecutor(@Autowired BizDConfs bizDConfs,
                                          @Autowired DAGClientPool dagClientPool) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("olympicene-taskRun-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(30, 100, 100000,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(6000), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        TaskDecoratingExecutorServiceDecorator decorator = new TaskDecoratingExecutorServiceDecorator(threadPoolExecutor);
        decorator.setTaskDecoratorAssemblerList(List.of(new ShareMdcFeatureDecoratorAssembler()));

        return new RuntimeExecutorServiceProxy(
                bizDConfs,
                dagClientPool.getRuntimeExecutorClientMapping(),
                decorator
        );
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService callbackExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("olympicene-callback-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(30, 100, 100000,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(6000), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        TaskDecoratingExecutorServiceDecorator decorator = new TaskDecoratingExecutorServiceDecorator(threadPoolExecutor);
        decorator.setTaskDecoratorAssemblerList(List.of(new ShareMdcFeatureDecoratorAssembler()));
        return decorator;
    }

    @PostConstruct
    public void registrySubTypesForMapper() {
        ObjectMapperFactory.registerSubtypes(
                new NamedType(FunctionTask.class, "function"),
                new NamedType(ChoiceTask.class, "choice"),
                new NamedType(ForeachTask.class, "foreach"),
                new NamedType(PassTask.class, "pass"),
                new NamedType(SuspenseTask.class, "suspense"),
                new NamedType(ReturnTask.class, "return")
        );
    }

}
