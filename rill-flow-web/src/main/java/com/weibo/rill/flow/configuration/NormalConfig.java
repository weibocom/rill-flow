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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.rill.flow.service.decorator.ShareMdcFeatureDecoratorAssembler;
import com.weibo.rill.flow.service.decorator.TaskDecoratingExecutorServiceDecorator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.*;


@Configuration
public class NormalConfig {
    @Bean(destroyMethod = "shutdown")
    public ExecutorService clientPoolExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("flow-client-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(100, 100, 100000,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(100), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        TaskDecoratingExecutorServiceDecorator decorator = new TaskDecoratingExecutorServiceDecorator(threadPoolExecutor);
        decorator.setTaskDecoratorAssemblerList(List.of(new ShareMdcFeatureDecoratorAssembler()));
        return decorator;
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService multiRedoExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("flow-redo-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(3, 100, 100000,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(60000), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        TaskDecoratingExecutorServiceDecorator decorator = new TaskDecoratingExecutorServiceDecorator(threadPoolExecutor);
        decorator.setTaskDecoratorAssemblerList(List.of(new ShareMdcFeatureDecoratorAssembler()));
        return decorator;
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService statisticExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("olympicene-statistic-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(20, 100, 100000,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10000), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        TaskDecoratingExecutorServiceDecorator decorator = new TaskDecoratingExecutorServiceDecorator(threadPoolExecutor);
        decorator.setTaskDecoratorAssemblerList(List.of(new ShareMdcFeatureDecoratorAssembler()));

        return decorator;
    }
}
