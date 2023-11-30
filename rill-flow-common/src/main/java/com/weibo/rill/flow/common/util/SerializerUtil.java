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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.ddl.serialize.ObjectMapperFactory;


public class SerializerUtil {
    private static final ObjectMapper MAPPER = ObjectMapperFactory.getJSONMapper();

    public static String serializeToString(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (Exception e) {
            throw new TaskException(BizError.ERROR_DATA_FORMAT, e);
        }
    }

    private SerializerUtil() {

    }
}
