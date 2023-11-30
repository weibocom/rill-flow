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

package com.weibo.rill.flow.olympicene.traversal.helper;

import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

public class DefaultStasher implements Stasher {

    @Override
    public boolean stash(String executionId, Pair<TaskInfo, Map<String, Object>> taskInfoToContext) {
        return false;
    }

    @Override
    public boolean needStash(String executionId, TaskInfo taskInfo, Map<String, Object> context) {
        return false;
    }

    @Override
    public boolean needStashFlow(DAGInfo dagInfo, DAGStatus dagStatus) {
        return false;
    }

    @Override
    public boolean stashFlow(DAGInfo wholeDagInfo, Map<String, Object> dagContext) {
        return false;
    }
}
