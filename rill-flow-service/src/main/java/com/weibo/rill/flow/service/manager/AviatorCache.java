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

package com.weibo.rill.flow.service.manager;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;


@Slf4j
@Service
public class AviatorCache {
    @Autowired
    private SwitcherManager switcherManagerImpl;

    private final LoadingCache<String, Expression> aviatorExpression = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(new CacheLoader<>() {
                @Override
                public Expression load(String script) {
                    try {
                        return AviatorEvaluator.compile(script);
                    } catch (Exception e) {
                        log.error("aviatorExpression fails, script:{}", script, e);
                        return null;
                    }
                }
            });

    public Expression getAviatorExpression(String script) {
        if (!switcherManagerImpl.getSwitcherState("ENABLE_AVIATOR_COMPILE_EXPRESSION_CACHE")) {
            return AviatorEvaluator.compile(script);
        }

        try {
            return aviatorExpression.get(script);
        } catch (ExecutionException e) {
            log.warn("getAviatorExpression fails, script:{}", script, e);
            throw new TaskException(BizError.ERROR_DATA_RESTRICTION, "get aviator script expression fails");
        }
    }
}
