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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;

@Slf4j
public class CommonUtil {

    public static List<Integer> str2IntList(String s) {
        List<Integer> uids = new ArrayList<>();
        if (s != null) {
            for (String m : s.split(",")) {
                int id = str2int(m.trim());
                if (id > 0) {
                    uids.add(id);
                }
            }
        }
        return uids;

    }

    public static int str2int(String s) {
        return str2int(s, 0);
    }

    public static long str2long(String s) {
        return str2long(s, 0L);
    }

    private static long str2long(String s, long defaultValue) {
        if (s == null) {
            return defaultValue;
        }

        try {
            return NumberUtils.toLong(s);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static int str2int(String s, int defaultValue) {
        if (s == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Map<String, Object> beanToMap(Object obj, Set<String> ignoreFields) {
        Map<String, Object> map = new HashMap<>();
        Set<String> initIgnoreFields = new HashSet<>();
        initIgnoreFields.add("class");
        Optional.ofNullable(ignoreFields).ifPresent(initIgnoreFields::addAll);

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor property : propertyDescriptors) {
                String key = property.getName();
                if (!initIgnoreFields.contains(key)) {
                    Method getter = property.getReadMethod();
                    Object value = getter.invoke(obj);

                    if (value != null) {
                        map.put(key, value);
                    }
                }
            }
            return map;
        } catch (Exception e) {
            log.warn("bean to map failed. bean:{}, ignoreFiled:{}, msg:{}.", obj, ignoreFields, e.getMessage());
            return Collections.emptyMap();
        }
    }


    private CommonUtil() {

    }
}
