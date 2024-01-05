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

package com.weibo.rill.flow.olympicene.ddl.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class YAMLMapper {
    private static final ObjectMapper MAPPER = ObjectMapperFactory.getYamlMapper();

    private YAMLMapper() {
    }

    public static <T> T parseObject(final String text, final Class<T> type) throws IOException {
        return MAPPER.readValue(text, type);
    }

    public static <T> T parseObject(final byte[] bytes, final Class<T> type) throws IOException {
        return MAPPER.readValue(bytes, type);
    }

    public static <T> byte[] toBytes(final T t) throws IOException {
        return MAPPER.writeValueAsBytes(t);
    }

}
