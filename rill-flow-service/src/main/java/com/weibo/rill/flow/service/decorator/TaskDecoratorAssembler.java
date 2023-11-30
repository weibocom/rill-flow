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

/**
 * 任务装饰器的装配器。该接口可以为一个任务装配一个装饰器, 使任务执行的时候额外执行一些逻辑。经过装饰的任务执行的过程是:
 * <p>
 * <pre>
 * |--- B -->|------ real task ----->|--- A -->|
 *
 * B: 真实任务执行开始前的装饰器的逻辑
 * A: 真实任务执行完成后的装饰器的逻辑
 * </pre>
 *
 * @author jerry 16-3-7.
 */
public interface TaskDecoratorAssembler {

    /**
     * 给任务组装上装饰器, 并返回经过装饰的任务。
     * 
     * @param task 原始任务
     * @return 被装饰器装饰之后的任务
     */
    Runnable assembleDecorator(Runnable task);

}
