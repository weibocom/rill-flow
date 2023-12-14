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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author zilong6
 */
public class RedisCodecUtil {

    public static final Function<Object, byte[]> castToByte = o -> {
        if (o instanceof byte[]) {
            return (byte[]) o;
        } else {
            throw new IllegalArgumentException(o + " cannot cast to byte[]");
        }
    };

    public static final Function<byte[], String> convertToString = bytes -> new String(bytes, StandardCharsets.UTF_8);

    public static final Function<Object, String> TO_STRING = object -> {
        if (object instanceof String) {
            return (String) object;
        } else if (object instanceof byte[]) {
            return new String((byte[]) object, StandardCharsets.UTF_8);
        } else {
            return String.valueOf(object);
        }
    };

    public static String getAsString(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return (String) object;
        } else if (object instanceof byte[]) {
            return TO_STRING.apply(object);
        } else {
            return object.toString();
        }
    }

    public static List<String> getList(Object object) {
        return getList(object, TO_STRING);
    }

    public static <E> List<E> getList(Object object, Function<Object, E> func) {
        if (object instanceof List<?> list) {
            List<E> result = new ArrayList<>(list.size());
            for (Object o : list) {
                try {
                    result.add(func.apply(o));
                } catch (Exception e) {
                    return null;
                }
            }
            return result;
        } else {
            return null;
        }
    }

    public static Map<String, String> getAsMap(Object object) {
        return getAsMap(object, TO_STRING, TO_STRING);
    }

    public static <K, V> Map<K, V> getAsMap(Object object, Function<Object, K> keyFunc, Function<Object, V> valFunc) {
        if (object instanceof List<?> list) {

            if (list.size() % 2 != 0) {
                return null;
            }

            Map<K, V> result = new HashMap<>(list.size() / 2);
            for (int i = 0; i < list.size(); i += 2) {
                Object keyElem = list.get(i);
                Object valElem = list.get(i + 1);

                try {
                    result.put(keyFunc.apply(keyElem), valFunc.apply(valElem));
                } catch (Exception e) {
                    return null;
                }
            }
            return result;
        } else {
            return null;
        }
    }

}
