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

package com.weibo.rill.flow.service.plugin;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.weibo.rill.flow.common.model.ProfileType;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.service.util.ProfileUtil;
import com.weibo.rill.flow.service.util.PrometheusActions;
import com.weibo.rill.flow.service.util.PrometheusUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

import java.util.List;
import java.util.Optional;
import java.util.Set;


@Plugin(
        name = "CustomizedExceptionFilter",
        category = "Core",
        elementType = "filter",
        printObject = true
)
public class CustomizedExceptionFilter extends AbstractFilter {
    private static final ProfileType DAG_SYS_EXP = new ProfileType("dag_sys_exception");
    private static final ProfileType DAG_TENANT_EXP = new ProfileType("dag_tenant_exception");
    private static final String CONNECTOR = "_";
    private static final String TOTAL_NAME_FORMAT = "%s_total";
    private static final String EXP_NAME_FORMAT = "%s_%s";
    private static final Set<String> SYS_IGNORE_LOGS = Sets.newConcurrentHashSet();

    private static final String DAG_SYS_EXP_STR = "dag_sys_exception_";

    private static final String DAG_TENANT_EXP_STR = "dag_tenant_exception_";


    private CustomizedExceptionFilter() {
        super();
    }

    private CustomizedExceptionFilter(Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
    }

    public static void setSysProfileIgnoreLogs(Set<String> logs) {
        SYS_IGNORE_LOGS.clear();
        Optional.ofNullable(logs).ifPresent(SYS_IGNORE_LOGS::addAll);
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
        recordProfileLog(level, msg, params);
        return Result.NEUTRAL;
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
        recordProfileLog(level, msg, t);
        return Result.NEUTRAL;
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        recordProfileLog(level, msg, t);
        return Result.NEUTRAL;
    }

    public void recordProfileLog(Level level, Object msg, Object... params) {
        try {
            if (!level.isMoreSpecificThan(Level.WARN)) {
                return;
            }

            String serviceId = null;
            String exceptionType = null;
            List<String> logMessages = Lists.newArrayList();
            Optional.ofNullable(msg).filter(it -> it instanceof String).map(it -> (String) it).ifPresent(logMessages::add);
            for (Object param : params) {
                if (param instanceof String) {
                    String paramString = (String) param;
                    logMessages.add(paramString);
                    if (ExecutionIdUtil.isExecutionId(paramString)) {
                        serviceId = ExecutionIdUtil.getServiceId(paramString);
                    }
                } else if (param instanceof Throwable) {
                    Throwable t = (Throwable) param;
                    exceptionType = t.getClass().getSimpleName();
                    logMessages.add(t.getMessage());
                }
            }

            boolean recordSysProfile = logMessages.stream()
                    .filter(StringUtils::isNotEmpty)
                    .allMatch(message -> SYS_IGNORE_LOGS.stream().filter(StringUtils::isNotEmpty).noneMatch(message::contains));
            if (StringUtils.isNotEmpty(serviceId)) {
                doRecordProfile(DAG_TENANT_EXP, level.name() + CONNECTOR + serviceId, exceptionType);
                // 记录prometheus
                doRecordPrometheus(DAG_TENANT_EXP_STR, level.name() + CONNECTOR + serviceId, exceptionType);
            }
            if (recordSysProfile) {
                doRecordProfile(DAG_SYS_EXP, level.name(), exceptionType);
                // 记录prometheus
                doRecordPrometheus(DAG_SYS_EXP_STR, level.name(), exceptionType);
            }
        } catch (Exception e) {
            // do nothing
        }
    }

    private void doRecordProfile(ProfileType type, String namePrefix, String exceptionType) {
        String totalName = String.format(TOTAL_NAME_FORMAT, namePrefix);
        ProfileUtil.count(type, totalName, System.currentTimeMillis(), 1);
        if (StringUtils.isNotEmpty(exceptionType)) {
            String exceptionName = String.format(EXP_NAME_FORMAT, namePrefix, exceptionType);
            ProfileUtil.count(type, exceptionName, System.currentTimeMillis(), 1);
        }
    }

    private void doRecordPrometheus(String typeName, String namePrefix, String exceptionType) {
        String totalName = String.format(TOTAL_NAME_FORMAT, namePrefix);
        PrometheusUtil.count(PrometheusActions.METER_PREFIX + typeName + totalName);
        if (StringUtils.isNotEmpty(exceptionType)) {
            String exceptionName = String.format(EXP_NAME_FORMAT, namePrefix, exceptionType);
            PrometheusUtil.count(PrometheusActions.METER_PREFIX + typeName + exceptionName);
        }
    }

    @PluginFactory
    public static CustomizedExceptionFilter createFilter(@PluginAttribute("onMatch") Result match, @PluginAttribute("onMismatch") Result mismatch) {
        return new CustomizedExceptionFilter(match, mismatch);
    }

    @PluginFactory
    public static CustomizedExceptionFilter createFilter() {
        return new CustomizedExceptionFilter();
    }
}
