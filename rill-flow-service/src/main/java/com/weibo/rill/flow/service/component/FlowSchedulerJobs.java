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

package com.weibo.rill.flow.service.component;

import com.weibo.rill.flow.service.statistic.BusinessTimeChecker;
import com.weibo.rill.flow.service.statistic.SystemMonitorStatistic;
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Service
@EnableScheduling
public class FlowSchedulerJobs {
    @Autowired
    private BusinessTimeChecker businessTimeChecker;
    @Autowired
    private SystemMonitorStatistic systemMonitorStatistic;
    @Autowired
    private TenantTaskStatistic tenantTaskStatistic;

    @Scheduled(cron = "0/3 * * * * *")
    public void businessTimeCheck() {
        businessTimeChecker.timeCheckWithRequestId();
    }

    @Scheduled(cron = "0/10 * * * * *")
    public void executionRateMonitor() {
        systemMonitorStatistic.logExecutionStatus();
    }

    @Scheduled(cron = "0/10 * * * * *")
    public void tenantTaskRecord() {
        tenantTaskStatistic.setBusinessValue();
    }
}
