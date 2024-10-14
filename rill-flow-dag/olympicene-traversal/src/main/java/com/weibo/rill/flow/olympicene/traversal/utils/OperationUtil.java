package com.weibo.rill.flow.olympicene.traversal.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

@Slf4j
public class OperationUtil {
    private OperationUtil() {}

    public static final BiConsumer<Runnable, Integer> OPERATE_WITH_RETRY = (operation, retryTimes) -> {
        int exceptionCatchTimes = retryTimes;
        for (int i = 1; i <= exceptionCatchTimes; i++) {
            try {
                operation.run();
                return;
            } catch (Exception e) {
                log.warn("operateWithRetry fails, invokeTimes:{}", i, e);
            }
        }

        operation.run();
    };
}
