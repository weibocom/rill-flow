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

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

public class UuidUtil {
    private static final long EPOCH_MILLIS;
    private static final TimeBasedGenerator UUID_GENERATOR;

    static {
        Calendar uuidEpoch = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        uuidEpoch.clear();
        /**
         * 原因见 {@link UUID#timestamp()}
         */
        uuidEpoch.set(1582, Calendar.OCTOBER, 15, 0, 0, 0);
        EPOCH_MILLIS = uuidEpoch.getTime().getTime();

        UUID_GENERATOR = Generators.timeBasedGenerator();
    }

    public static long timestamp(String uuidString) {
        return timestamp(parse(uuidString));
    }

    public static long timestamp(UUID uuid) {
        return  (uuid.timestamp() / 10000L) + EPOCH_MILLIS;
    }

    public static UUID uuid() {
        return UUID_GENERATOR.generate();
    }

    public static String jobId() {
        return uuid().toString().toLowerCase();
    }

    public static UUID parse(String uuidString) {
        return UUID.fromString(uuidString);
    }

    public static boolean isValid(String uuidString) {
        try {
            UUID.fromString(uuidString);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private UuidUtil() {
    }
}
