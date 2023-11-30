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

package com.weibo.rill.flow.common.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class BusinessHeapStatus {
    private String serviceId;
    private long collectTime; // 数据收集时间 单位: 毫秒
    private int statisticDuration; // 统计时长 单位: 分钟
    private long statisticTimePeriodStartTime; // 信息统计时间段的开始时间 单位: 毫秒
    private long statisticTimePeriodEndTime; // 信息统计时间段的结束时间 单位: 毫秒
    private Long runningCount;
    private Long successCount;
    private Long failedCount;
}
