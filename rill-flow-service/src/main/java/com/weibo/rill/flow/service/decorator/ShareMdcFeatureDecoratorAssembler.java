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

package com.weibo.rill.flow.service.decorator;

import org.slf4j.MDC;

import java.util.Map;

/**
 * This class will wrap the runnable command so that it can will share the MDC context with the
 * current thread.
 *
 * @author jerry 16-3-7.
 */
public class ShareMdcFeatureDecoratorAssembler implements TaskDecoratorAssembler {

    @Override
    public Runnable assembleDecorator(Runnable task) {
        return new ShareMdcTaskDecorator(MDC.getCopyOfContextMap(), task);
    }

    private static final class ShareMdcTaskDecorator implements Runnable {
        private final Map<String, String> context;
        private final Runnable command;

        ShareMdcTaskDecorator(Map<String, String> context, Runnable command) {
            this.context = context;
            this.command = command;
        }

        @Override
        public void run() {
            Map<String, String> previous = MDC.getCopyOfContextMap();
            if (context == null) {
                MDC.clear();
            } else {
                MDC.setContextMap(context);
            }

            try {
                command.run();
            } finally {
                if (previous == null) {
                    MDC.clear();
                } else {
                    MDC.setContextMap(previous);
                }
            }
        }
    }

}
