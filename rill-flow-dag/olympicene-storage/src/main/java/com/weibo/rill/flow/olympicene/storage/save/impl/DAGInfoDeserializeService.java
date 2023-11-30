package com.weibo.rill.flow.olympicene.storage.save.impl;

import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;

import java.util.List;
import java.util.Map;

public interface DAGInfoDeserializeService {
    DAGInfo deserializeBaseDagInfo(List<List<byte[]>> dagInfoByte);
    Map<String, Map<String, TaskInfo>> getTaskNameToSubTasksMap(List<List<List<byte[]>>> dagInfoByte);
}
