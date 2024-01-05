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

package com.weibo.rill.flow.olympicene.traversal.config;

import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.event.Callback;
import com.weibo.rill.flow.olympicene.core.result.DAGResultHandler;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import com.weibo.rill.flow.olympicene.traversal.DAGOperations;
import com.weibo.rill.flow.olympicene.traversal.DAGTraversal;
import com.weibo.rill.flow.olympicene.traversal.Olympicene;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo;
import com.weibo.rill.flow.olympicene.traversal.checker.TimeChecker;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.olympicene.traversal.helper.DefaultStasher;
import com.weibo.rill.flow.olympicene.traversal.helper.SameThreadExecutorService;
import com.weibo.rill.flow.olympicene.traversal.helper.Stasher;
import com.weibo.rill.flow.olympicene.traversal.mappings.InputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPath;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.runners.*;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class OlympiceneFacade {

    public static Olympicene build(DAGInfoStorage dagInfoStorage, DAGContextStorage dagContextStorage,
                                   Callback<DAGCallbackInfo> callback, DAGDispatcher dagDispatcher,
                                   DAGStorageProcedure dagStorageProcedure, TimeChecker timeChecker,
                                   SwitcherManager switcherManager) {
        ExecutorService executor = SameThreadExecutorService.INSTANCE;
        return build(dagInfoStorage, dagContextStorage, dagStorageProcedure, callback, null,
                dagDispatcher, timeChecker, executor, switcherManager);
    }

    public static Olympicene build(DAGInfoStorage dagInfoStorage, DAGContextStorage dagContextStorage, DAGStorageProcedure dagStorageProcedure,
                                   Callback<DAGCallbackInfo> callback, DAGResultHandler dagResultHandler, DAGDispatcher dagDispatcher,
                                   TimeChecker timeChecker, ExecutorService executor, SwitcherManager switcherManager) {
        JSONPathInputOutputMapping jsonPathInputOutputMapping = new JSONPathInputOutputMapping();

        DefaultStasher stasher = new DefaultStasher();
        DAGRunner dagRunner = new DAGRunner(dagContextStorage, dagInfoStorage, dagStorageProcedure);
        dagRunner.setStasher(stasher);
        TimeCheckRunner timeCheckRunner = new TimeCheckRunner(timeChecker, dagInfoStorage, dagContextStorage, dagStorageProcedure);
        Map<String, TaskRunner> taskRunners = buildTaskRunners(dagInfoStorage, dagContextStorage, dagDispatcher,
                jsonPathInputOutputMapping, jsonPathInputOutputMapping, dagStorageProcedure, stasher, switcherManager);

        DAGTraversal dagTraversal = new DAGTraversal(dagContextStorage, dagInfoStorage, dagStorageProcedure, executor);
        DAGOperations dagOperations = new DAGOperations(executor, taskRunners, dagRunner, timeCheckRunner, dagTraversal, callback, dagResultHandler);
        dagTraversal.setDagOperations(dagOperations);
        dagTraversal.setStasher(stasher);
        timeCheckRunner.setDagOperations(dagOperations);
        return new Olympicene(dagInfoStorage, dagOperations, executor, dagResultHandler);
    }

    public static Map<String, TaskRunner> buildTaskRunners(DAGInfoStorage dagInfoStorage,
                                                           DAGContextStorage dagContextStorage,
                                                           DAGDispatcher dagDispatcher,
                                                           InputOutputMapping mapping,
                                                           JSONPath jsonPath,
                                                           DAGStorageProcedure dagStorageProcedure,
                                                           Stasher stasher,
                                                           SwitcherManager switcherManager) {
        PassTaskRunner passTaskRunner = new PassTaskRunner(mapping, dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);
        FunctionTaskRunner functionTaskRunner = new FunctionTaskRunner(dagDispatcher, mapping, dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);
        SuspenseTaskRunner suspenseTaskRunner = new SuspenseTaskRunner(mapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
        ReturnTaskRunner returnTaskRunner = new ReturnTaskRunner(mapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
        ForeachTaskRunner foreachTaskRunner = new ForeachTaskRunner(mapping, jsonPath, dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);
        foreachTaskRunner.setStasher(stasher);
        ChoiceTaskRunner choiceTaskRunner = new ChoiceTaskRunner(mapping, dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);

        Map<String, TaskRunner> runners = Maps.newConcurrentMap();
        runners.put(TaskCategory.FUNCTION.getValue(), functionTaskRunner);
        runners.put(TaskCategory.CHOICE.getValue(), choiceTaskRunner);
        runners.put(TaskCategory.FOREACH.getValue(), foreachTaskRunner);
        runners.put(TaskCategory.SUSPENSE.getValue(), suspenseTaskRunner);
        runners.put(TaskCategory.PASS.getValue(), passTaskRunner);
        runners.put(TaskCategory.RETURN.getValue(), returnTaskRunner);
        return runners;
    }
}
