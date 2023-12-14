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

package com.weibo.rill.flow.service.decorator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 该类是{@link ExecutorService}的一个装饰器。能向该装饰器包装的{@link ExecutorService}里面提交的任务前后添加一系列期望的功能, 比如任务计数, 计时,
 * 串{@code request_id}等, 这些功能都以任务的装饰器的方式提供, 见{@link TaskDecoratorAssembler}。
 *
 * @author jerry 16-3-7.
 */
public class TaskDecoratingExecutorServiceDecorator extends AbstractExecutorService {

    private final ExecutorService underlyingExecutorService;
    private final Deque<TaskDecoratorAssembler> taskDecoratorAssemblerList = new ArrayDeque<>();

    public TaskDecoratingExecutorServiceDecorator(ExecutorService underlyingExecutorService) {
        if (underlyingExecutorService == null) {
            throw new IllegalArgumentException("underlying executor service must not be null");
        }
        this.underlyingExecutorService = underlyingExecutorService;
    }

    @Override
    public void shutdown() {
        underlyingExecutorService.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return underlyingExecutorService.shutdownNow();
    }

    /**
     * 设置用户期望的任务装饰器的装配器, 提交的任务都会经过这些装配器装配上他们各自的装饰器, 从而可以执行一些期望的额外逻辑。
     * <p>
     * 所有的装配器组织成一个装配器列表, 列表起始位置的装配器装配的装饰器在最外层, 末尾的装配器装配的装饰器在最里层。
     * <p>
     * <pre>
     * D0:   |--B0-->+  ..................................................  +--A0-->|
     *               |                                                      |
     * D1:           +--B1-->+  ..................................  +--A1-->+
     *                       |                                      |
     * ... :                 +-- ..            ...             .. --+
     *                             |                          |
     * Dn:                         +--->+   .........    +--->+
     *                                  |                |
     * real task:                       +------ RT ----->+
     * </pre>
     * 
     * @param taskDecoratorAssemblerList 任务装饰器的配置器列表, 列表中的每一个配置器都可以为任务装配上特定的装饰器,
     *        使得任务运行的时候可以运行一些额外的逻辑。
     */
    public synchronized void setTaskDecoratorAssemblerList(List<TaskDecoratorAssembler> taskDecoratorAssemblerList) {
        if (taskDecoratorAssemblerList != null) {
            this.taskDecoratorAssemblerList.clear();
            this.taskDecoratorAssemblerList.addAll(taskDecoratorAssemblerList);
        }
    }

    public List<TaskDecoratorAssembler> getTaskDecoratorAssemblerList() {
        return List.copyOf(taskDecoratorAssemblerList);
    }

    @Override
    public boolean isShutdown() {
        return underlyingExecutorService.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return underlyingExecutorService.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return underlyingExecutorService.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException();
        }
        Runnable eventualCommand = task;

        Iterator<TaskDecoratorAssembler> taskDecoratorProviderIterator = taskDecoratorAssemblerList.descendingIterator();

        while (taskDecoratorProviderIterator.hasNext()) {
            TaskDecoratorAssembler taskDecoratorAssembler = taskDecoratorProviderIterator.next();
            eventualCommand = taskDecoratorAssembler.assembleDecorator(eventualCommand);
        }
        underlyingExecutorService.execute(eventualCommand);
    }


}
