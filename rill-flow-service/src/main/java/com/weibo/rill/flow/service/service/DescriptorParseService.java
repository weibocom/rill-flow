package com.weibo.rill.flow.service.service;

import com.weibo.rill.flow.olympicene.core.model.dag.DAG;

public interface DescriptorParseService {
    String processWhenGetDescriptor(String descriptor);
    void processWhenSetDAG(DAG dag);
}
