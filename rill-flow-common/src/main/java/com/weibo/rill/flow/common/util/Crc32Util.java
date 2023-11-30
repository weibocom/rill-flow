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

package com.weibo.rill.flow.common.util;

import java.util.zip.CRC32;

public class Crc32Util {
    private Crc32Util() {
        throw new IllegalStateException("Utility class");
    }

    private static final ThreadLocal<CRC32> crc32 = ThreadLocal.withInitial(CRC32::new);

    public static long crc32(byte[] b) {
        CRC32 c = crc32.get();
        c.reset();
        c.update(b);
        return c.getValue();
    }
}
