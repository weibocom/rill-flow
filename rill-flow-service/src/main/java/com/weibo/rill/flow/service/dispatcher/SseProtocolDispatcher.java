package com.weibo.rill.flow.service.dispatcher;

import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import org.springframework.stereotype.Service;

@Service("sseDispatcher")
public class SseProtocolDispatcher implements DispatcherExtension {
    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        return "";
    }

    @Override
    public String getName() {
        return "sse";
    }
}
