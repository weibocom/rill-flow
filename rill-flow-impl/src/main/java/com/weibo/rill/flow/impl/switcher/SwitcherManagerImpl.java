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

package com.weibo.rill.flow.impl.switcher;

import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class SwitcherManagerImpl implements SwitcherManager {
    private static final Map<String, Field> switcherFieldMap = new ConcurrentHashMap<>();

    @Override
    public boolean getSwitcherState(String switcherName) {
        try {
            Field switcher = switcherFieldMap.get(switcherName);
            if (switcher == null) {
                switcher = Switchers.class.getDeclaredField(switcherName);
                switcherFieldMap.put(switcherName, switcher);
            }
            AtomicBoolean atomicBoolean = (AtomicBoolean) switcher.get(null);
            return atomicBoolean.get();
        } catch (NoSuchFieldException e) {
            log.warn("switcher not exist: {}", switcherName, e);
        } catch (IllegalAccessException e) {
            log.warn("switcher field not boolean: {}", switcherName, e);
        }
        return false;
    }
}
