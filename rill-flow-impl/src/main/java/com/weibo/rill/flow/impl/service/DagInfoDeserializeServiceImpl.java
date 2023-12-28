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

package com.weibo.rill.flow.impl.service;

import com.google.common.collect.Maps;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInvokeMsg;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDeserializeService;
import com.weibo.rill.flow.olympicene.storage.save.impl.DagStorageSerializer;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.weibo.rill.flow.olympicene.storage.save.impl.DAGInfoDAO.*;

@Service
public class DagInfoDeserializeServiceImpl implements DAGInfoDeserializeService {
    @Override
    public DAGInfo deserializeBaseDagInfo(List<List<byte[]>> dagInfoByte) {
        if (CollectionUtils.isEmpty(dagInfoByte) || dagInfoByte.size() != 2 || CollectionUtils.isEmpty(dagInfoByte.get(1))) {
            return null;
        }
        DAGInfo dagInfo = new DAGInfo();
        DagStorageSerializer.deserializeHash(dagInfoByte.get(1)).forEach((key, value) -> {
            if (EXECUTION_ID.equals(key)) {
                dagInfo.setExecutionId((String) value);
            } else if (DAG_DESCRIBER.equals(key)) {
                dagInfo.setDag((DAG) value);
            } else if (DAG_INVOKE_MSG.equals(key)) {
                dagInfo.setDagInvokeMsg((DAGInvokeMsg) value);
            } else if (DAG_STATUS.equals(key)) {
                dagInfo.setDagStatus((DAGStatus) value);
            } else if (key.startsWith(TASK_FIELD_PREFIX)) {
                dagInfo.setTask(((TaskInfo) value).getName(), (TaskInfo) value);
            }
        });
        return dagInfo;
    }

    public Map<String, Map<String, TaskInfo>> getTaskNameToSubTasksMap(List<List<List<byte[]>>> dagInfoByte) {
        if (dagInfoByte.size() < 2) {
            return Maps.newHashMap();
        }

        Map<String, Map<String, TaskInfo>> taskNameToSubTasks = Maps.newHashMap();
        dagInfoByte.subList(1, dagInfoByte.size()).stream()
                .filter(it -> CollectionUtils.isNotEmpty(it) && it.size() == 2)
                .filter(it -> CollectionUtils.isNotEmpty(it.get(0)) && it.get(0).size() == 2)
                .filter(it -> CollectionUtils.isNotEmpty(it.get(1)))
                .forEach(subTaskSetting -> {
                    List<byte[]> parentSetting = subTaskSetting.get(0);
                    List<byte[]> subTaskInfos = subTaskSetting.get(1);
                    Map<String, TaskInfo> subTaskMap = new LinkedHashMap<>();
                    DagStorageSerializer.deserializeHash(subTaskInfos)
                            .forEach((taskName, taskInfo) -> subTaskMap.put(((TaskInfo) taskInfo).getName(), (TaskInfo) taskInfo));
                    taskNameToSubTasks.put(DagStorageSerializer.getString(parentSetting.get(1)), subTaskMap);
                });
        return taskNameToSubTasks;
    }
}
