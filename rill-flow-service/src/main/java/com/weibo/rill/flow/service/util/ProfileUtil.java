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

import com.weibo.rill.flow.common.constant.AccessStatisticResult;
import com.weibo.rill.flow.common.constant.ProfileConstants;
import com.weibo.rill.flow.common.model.ProfileType;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监控统计类, 为了解除对api-commons的依赖, borrow code from
 * api-commons#com.weibo.rill.flow.commons.profile#ProfileUtil
 *
 * @author liyan23
 */
public class ProfileUtil {
    private static final Logger log = LoggerFactory.getLogger(ProfileUtil.class);
    private static final Logger profileLogger = LoggerFactory.getLogger("profile");

    private static final String SEPARATE = "\\|";

    private static ScheduledExecutorService executorService;
    private static ConcurrentMap<String, AccessStatisticItem> accessStatistics = new ConcurrentHashMap<>();

    static {
        startLogging();
        Runtime.getRuntime().addShutdownHook(new Thread(ProfileUtil::stopLogging));
    }

    public static void startLogging() {
        stopLogging();
        executorService = Executors.newSingleThreadScheduledExecutor();
        // access statistic
        executorService.scheduleAtFixedRate(() -> logAccessStatistic(false), ProfileConstants.STATISTIC_PERIOD,
                ProfileConstants.STATISTIC_PERIOD, TimeUnit.SECONDS);
    }

    public static void stopLogging() {
        Optional.ofNullable(executorService).ifPresent(ExecutorService::shutdown);
        executorService = null;
        logAccessStatistic(true); // the last log
    }

    public static void accessStatistic(ProfileType profileType, String name, long currentTimeMillis, long costTimeMillis) {
        String key = profileType.getType() + "|" + name;

        try {
            AccessStatisticItem item = getStatisticItem(key, profileType, currentTimeMillis);
            item.statistic(currentTimeMillis, costTimeMillis);
        } catch (Exception e) {
            log.error("ProfileUtil accessStatistic error, type=" + profileType.getType() + ", name=" + name, e);
        }
    }

    private static AccessStatisticItem getStatisticItem(String key, ProfileType profileType, long currentTime) {
        AccessStatisticItem item = accessStatistics.get(key);

        if (item == null) {
            accessStatistics.putIfAbsent(key, new CostStatistic(profileType, currentTime, ProfileConstants.STATISTIC_PERIOD * 2));
            item = accessStatistics.get(key);
        }

        return item;
    }

    public static void count(ProfileType profileType, String name, long currentTimeMillis, int count) {
        String key = profileType.getType() + "|" + name;

        try {
            AccessStatisticItem item = getCounter(key, profileType, currentTimeMillis);
            item.statistic(currentTimeMillis, count);
        } catch (Exception e) {
            log.error("ProfileUtil count error, type=" + profileType.getType() + ", name=" + name, e);
        }

    }

    private static AccessStatisticItem getCounter(final String key, final ProfileType profileType, final long currentTimeMillis) {
        AccessStatisticItem item = accessStatistics.get(key);
        if (item == null) {
            accessStatistics.putIfAbsent(key, new Counter(profileType, currentTimeMillis, ProfileConstants.STATISTIC_PERIOD * 2));
            item = accessStatistics.get(key);
        }
        return item;
    }

    private static void logAccessStatistic(final boolean all) {
        DecimalFormat mbFormat = new DecimalFormat("#0.00");
        long currentTimeMillis = System.currentTimeMillis();

        for (Map.Entry<String, AccessStatisticItem> entry : accessStatistics.entrySet()) {
            AccessStatisticItem item = entry.getValue();

            AccessStatisticResult result = all
                    ? item.getStatisticResult(currentTimeMillis + 1000, ProfileConstants.STATISTIC_PERIOD + 1)
                    : item.getStatisticResult(currentTimeMillis, ProfileConstants.STATISTIC_PERIOD);

            item.clearStatistic(currentTimeMillis, ProfileConstants.STATISTIC_PERIOD);

            String key = entry.getKey();
            String[] keys = key.split(SEPARATE);
            if (keys.length != 2) {
                continue;
            }
            String type = keys[0];
            String name = keys[1];

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("type", type);
            jsonObject.put("name", name);
            jsonObject.put("slowThreshold", result.slowThreshold);
            if (result.totalCount == 0) {
                jsonObject.put("total_count", 0);
                jsonObject.put("slow_count", 0);
                jsonObject.put("avg_time", "0.00");
                jsonObject.put("interval1", 0);
                jsonObject.put("interval2", 0);
                jsonObject.put("interval3", 0);
                jsonObject.put("interval4", 0);
                jsonObject.put("interval5", 0);

            } else {
                jsonObject.put("total_count", result.totalCount);
                jsonObject.put("slow_count", result.slowCount);
                jsonObject.put("avg_time", mbFormat.format(result.costTime / result.totalCount));
                jsonObject.put("interval1", result.intervalCounts[0]);
                jsonObject.put("interval2", result.intervalCounts[1]);
                jsonObject.put("interval3", result.intervalCounts[2]);
                jsonObject.put("interval4", result.intervalCounts[3]);
                jsonObject.put("interval5", result.intervalCounts[4]);
            }
            String monitorInfo = jsonObject.toString();
            profileLogger.info(monitorInfo);
        }
    }

    public static abstract class AccessStatisticItem {
        volatile int currentIndex;
        AtomicInteger[] costTimes = null;
        AtomicInteger[] totalCounter = null;
        AtomicInteger[] slowCounter = null;
        int length;
        AtomicInteger[] interval1 = null;
        AtomicInteger[] interval2 = null;
        AtomicInteger[] interval3 = null;
        AtomicInteger[] interval4 = null;
        AtomicInteger[] interval5 = null;
        ProfileType profileType = null;

        public AccessStatisticItem(ProfileType profileType, long currentTimeMillis) {
            this(profileType, currentTimeMillis, ProfileConstants.STATISTIC_PERIOD * 2);
        }

        public AccessStatisticItem(ProfileType profileType, long currentTimeMillis, int length) {
            this.costTimes = initAtomicIntegerArr(length);
            this.totalCounter = initAtomicIntegerArr(length);
            this.slowCounter = initAtomicIntegerArr(length);
            this.length = length;
            this.interval1 = initAtomicIntegerArr(length);
            this.interval2 = initAtomicIntegerArr(length);
            this.interval3 = initAtomicIntegerArr(length);
            this.interval4 = initAtomicIntegerArr(length);
            this.interval5 = initAtomicIntegerArr(length);
            this.currentIndex = getIndex(currentTimeMillis, length);
            this.profileType = profileType;
        }

        private AtomicInteger[] initAtomicIntegerArr(int size) {
            AtomicInteger[] arrs = new AtomicInteger[size];
            for (int i = 0; i < arrs.length; i++) {
                arrs[i] = new AtomicInteger(0);
            }

            return arrs;
        }

        /**
         *
         * @param currentTimeMillis 此刻记录的时间 (ms)
         * @param value 这次操作的耗时 (ms)
         */
        void statistic(long currentTimeMillis, long value) {
            int currentIndex = getIndex(currentTimeMillis, length);

            ensureInitSlot(currentIndex);

            doStatistic(currentIndex, (int) value);
        }

        protected abstract void doStatistic(final int currentIndex, final int value);

        private void ensureInitSlot(final int tempIndex) {
            if (currentIndex != tempIndex) {
                synchronized (this) {
                    // 这一秒的第一条统计，把对应的存储位的数据置0
                    if (currentIndex != tempIndex) {
                        reset(tempIndex);
                        currentIndex = tempIndex;
                    }
                }
            }
        }

        private int getIndex(long currentTimeMillis, int periodSecond) {
            return (int) ((currentTimeMillis / 1000) % periodSecond);
        }

        private void reset(int index) {
            costTimes[index].set(0);
            totalCounter[index].set(0);
            slowCounter[index].set(0);
            interval1[index].set(0);
            interval2[index].set(0);
            interval3[index].set(0);
            interval4[index].set(0);
            interval5[index].set(0);
        }

        AccessStatisticResult getStatisticResult(long currentTimeMillis, int peroidSecond) {
            long currentTimeSecond = currentTimeMillis / 1000;
            currentTimeSecond--; // 当前这秒还没完全结束，因此数据不全，统计从上一秒开始，往前推移peroidSecond

            int startIndex = getIndex(currentTimeSecond * 1000, length);

            AccessStatisticResult result = new AccessStatisticResult();

            result.slowThreshold = profileType.getSlowThreshold();
            for (int i = 0; i < peroidSecond; i++) {
                int currentIndex = (startIndex - i + length) % length;

                result.costTime += costTimes[currentIndex].get();
                result.totalCount += totalCounter[currentIndex].get();
                result.slowCount += slowCounter[currentIndex].get();
                result.intervalCounts[0] += interval1[currentIndex].get();
                result.intervalCounts[1] += interval2[currentIndex].get();
                result.intervalCounts[2] += interval3[currentIndex].get();
                result.intervalCounts[3] += interval4[currentIndex].get();
                result.intervalCounts[4] += interval5[currentIndex].get();
                if (totalCounter[currentIndex].get() > result.maxCount) {
                    result.maxCount = totalCounter[currentIndex].get();
                } else if (totalCounter[currentIndex].get() < result.minCount || result.minCount == -1) {
                    result.minCount = totalCounter[currentIndex].get();
                }
            }

            return result;
        }

        void clearStatistic(long currentTimeMillis, int periodSecond) {
            long currentTimeSecond = currentTimeMillis / 1000;
            currentTimeSecond--; // 当前这秒还没完全结束，因此数据不全，统计从上一秒开始，往前推移peroidSecond

            int startIndex = getIndex(currentTimeSecond * 1000, length);

            for (int i = 0; i < periodSecond; i++) {
                int currentIndex = (startIndex - i + length) % length;

                reset(currentIndex);
            }
        }
    }

    static class CostStatistic extends AccessStatisticItem {

        public CostStatistic(final ProfileType profileType, final long currentTimeMillis) {
            super(profileType, currentTimeMillis);
        }

        public CostStatistic(final ProfileType profileType, final long currentTimeMillis, final int length) {
            super(profileType, currentTimeMillis, length);
        }

        @Override
        protected void doStatistic(final int currentIndex, final int value) {
            costTimes[currentIndex].addAndGet(value);
            totalCounter[currentIndex].incrementAndGet();

            if (value >= profileType.getSlowThreshold()) {
                slowCounter[currentIndex].incrementAndGet();
            }
            if (value < profileType.getInterval1()) {
                interval1[currentIndex].incrementAndGet();
            } else if (value < profileType.getInterval2()) {
                interval2[currentIndex].incrementAndGet();
            } else if (value < profileType.getInterval3()) {
                interval3[currentIndex].incrementAndGet();
            } else if (value < profileType.getInterval4()) {
                interval4[currentIndex].incrementAndGet();
            } else {
                interval5[currentIndex].incrementAndGet();
            }
        }
    }

    static class Counter extends AccessStatisticItem {

        public Counter(final ProfileType profileType, final long currentTimeMillis) {
            super(profileType, currentTimeMillis);
        }

        public Counter(final ProfileType profileType, final long currentTimeMillis, final int length) {
            super(profileType, currentTimeMillis, length);
        }

        @Override
        protected void doStatistic(final int currentIndex, final int value) {
            totalCounter[currentIndex].addAndGet(value);
        }
    }
}
