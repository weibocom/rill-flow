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

package com.weibo.rill.flow.olympicene.storage.redis.api;

import java.util.List;
import java.util.zip.CRC32;

/**
 * @author zilong6
 */
public class Crc32Sharding<T> implements Sharding<T> {

    private static final ThreadLocal<CRC32> crc32ThreadLocal = ThreadLocal.withInitial(CRC32::new);

    private static final Crc32Sharding<?> INSTANCE = new Crc32Sharding<>();

    @SuppressWarnings("unchecked")
    public static <T> Crc32Sharding<T> singleton() {
        return (Crc32Sharding<T>) INSTANCE;
    }

    @Override
    public T choose(List<T> clients, byte[] key) {
        if (clients.isEmpty()) {
            return null;
        } else if (clients.size() == 1) {
            return clients.get(0);
        } else if (key == null) {
            return null;
        } else {
            long value = getCrc32Value(key);

            int index = (int) (value % clients.size());
            return clients.get(index);
        }
    }

    private static long getCrc32Value(byte[] key) {
        CRC32 crc32 = crc32ThreadLocal.get();
        crc32.reset();
        crc32.update(key);
        long value = crc32.getValue();
        crc32.reset();
        return value;
    }
}
