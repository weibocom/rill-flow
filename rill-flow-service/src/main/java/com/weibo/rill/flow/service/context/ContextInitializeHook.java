package com.weibo.rill.flow.service.context;

public interface ContextInitializeHook<T>{
    T initialize(T context);
}
