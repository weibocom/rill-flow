package com.weibo.rill.flow.interfaces.executor;

import lombok.Getter;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolHolder {
    @Getter
    private static final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);
}
