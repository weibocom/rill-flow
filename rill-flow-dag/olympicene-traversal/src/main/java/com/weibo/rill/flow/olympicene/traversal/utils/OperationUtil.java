package com.weibo.rill.flow.olympicene.traversal.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.function.ObjIntConsumer;

@Slf4j
public class OperationUtil {
    private OperationUtil() {}

    public static final ObjIntConsumer<Runnable> OPERATE_WITH_RETRY = (operation, retryTimes) -> {
        for (int i = 1; i <= retryTimes; i++) {
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
