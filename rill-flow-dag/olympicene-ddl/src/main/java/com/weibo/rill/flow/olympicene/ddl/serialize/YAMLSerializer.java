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

import com.weibo.rill.flow.olympicene.core.exception.SerializationException;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;

import java.io.IOException;

public class YAMLSerializer implements Serializer {
    @Override
    public byte[] serialize(Object object) throws SerializationException {
        try {
            return YAMLMapper.toBytes(object);
        } catch (IOException e) {
            throw new SerializationException(DDLErrorCode.SERIALIZATION_FAIL.getCode(), e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException {
        try {
            return YAMLMapper.parseObject(bytes, type);
        } catch (IOException e) {
            throw new SerializationException(DDLErrorCode.SERIALIZATION_FAIL.getCode(), e);
        }
    }
}
