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

package com.weibo.rill.flow.service.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * bean初始化耗时统计
 * Created by xilong on 2022/4/1.
 */
@Slf4j
//@Component
public class BeanInitCost implements BeanPostProcessor {

    private static final ConcurrentHashMap<String, Long> BEAN_INIT_START_TIME = new ConcurrentHashMap<>();

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        BEAN_INIT_START_TIME.put(beanName, System.currentTimeMillis());
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Optional.ofNullable(BEAN_INIT_START_TIME.get(beanName))
                .ifPresent(startTime -> log.info("beanName:{} init cost:{}", beanName, System.currentTimeMillis() - startTime));
        return bean;
    }
}
