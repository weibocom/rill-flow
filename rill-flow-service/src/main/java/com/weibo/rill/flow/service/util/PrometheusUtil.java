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

package com.weibo.rill.flow.service.util;

import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @author xinyu55
 */
@Component
public class PrometheusUtil implements ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(PrometheusUtil.class);
    private static MeterRegistry registry;
    private static ApplicationContext applicationContext;


    private PrometheusUtil(MeterRegistry meterRegistry) {
        registry = meterRegistry;
    }

    /**
     * 次数统计，每次自增1，机器重启后会重置
     *
     * @param name 指标名称，prometheus中指标名称为name + "_total"
     * @param tags 标签，以key/value的形式。
     *             eg: name为test，tags参数为tag,x
     *             在prometheus中指标名称为“test_total"
     *             tag展示为tag="x"
     */
    public static void count(String name, String... tags) {
        if (enableCloseSwitch()) return;
        registry.counter(name, tags).increment();
    }

    private static boolean enableCloseSwitch() {
        SwitcherManager switcherManager = applicationContext.getBean(SwitcherManager.class);
        boolean enableOpenPrometheus = switcherManager.getSwitcherState("ENABLE_OPEN_PROMETHEUS");
        log.info("PrometheusUtil Switch ENABLE_OPEN_PROMETHEUS result: {}", enableOpenPrometheus);
        return !enableOpenPrometheus;
    }


    /**
     * 次数统计，每次自增count 机器重启后会重置
     *
     * @param name  指标名称，prometheus中指标名称为name + "_total"
     * @param count 增加的数量
     * @param tags  标签，以key/value的形式。
     *              eg: name为test，tags参数为tag,x
     *              在prometheus中指标名称为“test_total"
     *              tag展示为tag="x"
     */
    public static void count(String name, int count, String... tags) {
        if (enableCloseSwitch()) return;
        registry.counter(name, tags).increment(count);
    }

    /**
     * 耗时统计
     *
     * @param name           指标名称 prometheus中耗时总时间name + "_seconds_sum",请求总数name + "_seconds_count"
     * @param costTimeMillis 请求耗时，单位ms
     * @param tags           标签，以key/value的形式
     *                       eg: name为test_cost
     *                       prometheus中总耗时指标名称为“test_cost_seconds_sum"
     *                       总请求数指标名称为“test_cost_seconds_count"
     */
    public static void statisticsTotalTime(String name, long costTimeMillis, String... tags) {
        if (enableCloseSwitch()) return;
        Timer timer = registry.timer(name, tags);
        timer.record(costTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        PrometheusUtil.applicationContext = applicationContext;
    }
}
