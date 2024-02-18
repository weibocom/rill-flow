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

package com.weibo.rill.flow.service.dconfs;


import com.weibo.rill.flow.common.function.ResourceCheckConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BizDConfs {
    Map<String, ResourceCheckConfig> getResourceCheckIdToConfigBean();
    Map<String,String> getRedisBusinessIdToClientId();
    Map<String,String> getRedisServiceIdToClientId();
    Map<String,Integer> getRedisBusinessIdToFinishReserveSecond();
    Map<String,Integer> getRedisBusinessIdToUnfinishedReserveSecond();
    Map<String,Integer> getRedisBusinessIdToContextMaxLength();
    Map<String,Integer> getRedisBusinessIdToDAGInfoMaxLength();
    Map<String,String> getSwapBusinessIdToClientId();
    Map<String,Integer> getSwapBusinessIdToFinishReserveSecond();
    Map<String,Integer> getSwapBusinessIdToUnfinishedReserveSecond();
    Set<String> getRuntimeRedisUsageCheckIDs();
    Map<String,Integer> getRuntimeRedisStorageIdToMaxUsage();
    int getRuntimeRedisDefaultStorageMaxUsage();
    int getRuntimeRedisCustomizedStorageMaxUsage();
    int getResourceStatusStatisticTimeInSecond();
    Map<String,String> getResourceCheckIdToConfig();
    Map<String,Integer> getSubmitTrafficLimitIdToConfig();
    Set<String> getRuntimeHttpInvokeUrlBlackList();
    int getRuntimeSubmitContextMaxSize();
    int getRuntimeCallbackContextMaxSize();
    Map<String,Integer> getBusinessIdToStatisticLogSaveTimeInMinute();
    Map<String,Integer> getLogStartTimeOffsetInMinute();
    Map<String,Integer> getLogEndTimeOffsetInMinute();
    Set<String> getSysExpFilterIgnoreLogs();
    Map<String, List<String>> getTenantDefinedTaskInvokeProfileLog();
    int getFlowDAGMaxDepth();
    Map<String, String> getLongTermStorageBusinessIdToClientId();
    Map<String, Integer> getLongTermBusinessIdToReserveSecond();
    Map<String, String> getCustomizedBusinessIdToClientId();
    Map<String, String> getCustomizedServiceIdToClientId();
    Map<String, Integer> getCustomizedBusinessIdToReserveSecond();
    Map<String, String> getHttpClientBusinessIdToClientId();
    Map<String, String> getAuthSourceToKeyMap();
    Map<String, String> getTaskScoreExpWhenPop();

    Map<String, Integer> getRedisBusinessIdToRuntimeSubmitContextMaxSize();
    Map<String, Integer> getRedisBusinessIdToRuntimeCallbackContextMaxSize();
}
