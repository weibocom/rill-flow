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

package com.weibo.rill.flow.olympicene.spring.boot;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.rill.flow.olympicene.core.event.Callback;
import com.weibo.rill.flow.olympicene.core.model.task.TaskCategory;
import com.weibo.rill.flow.olympicene.core.result.DAGResultHandler;
import com.weibo.rill.flow.olympicene.core.runtime.DAGContextStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGInfoStorage;
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser;
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer;
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator;
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.ResourceDAGValidator;
import com.weibo.rill.flow.olympicene.spring.boot.exception.OlympicenceStarterException;
import com.weibo.rill.flow.olympicene.traversal.DAGOperations;
import com.weibo.rill.flow.olympicene.traversal.DAGTraversal;
import com.weibo.rill.flow.olympicene.traversal.Olympicene;
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo;
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker;
import com.weibo.rill.flow.olympicene.traversal.checker.TimeChecker;
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher;
import com.weibo.rill.flow.olympicene.traversal.helper.*;
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping;
import com.weibo.rill.flow.olympicene.traversal.runners.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ExecutorService;

@Slf4j
@Configuration
@AutoConfigureOrder(Integer.MIN_VALUE)
public class OlympiceneAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "dagInfoStorage")
    public DAGInfoStorage dagInfoStorage() {
        throw new OlympicenceStarterException("need customized DAGInfoStorage type bean");
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagContextStorage")
    public DAGContextStorage dagContextStorage() {
        throw new OlympicenceStarterException("need customized DAGContextStorage type bean");
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagStorageProcedure")
    public DAGStorageProcedure dagStorageProcedure() {
        throw new OlympicenceStarterException("need customized DAGStorageProcedure type bean");
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagCallback")
    public Callback<DAGCallbackInfo> dagCallback() {
        throw new OlympicenceStarterException("need customized Callback<DAGCallbackInfo> type bean");
    }

    @Bean
    @ConditionalOnMissingBean(name = "stasher")
    public Stasher stasher() {
        return new DefaultStasher();
    }

    @Bean
    @ConditionalOnMissingBean(name = "popper")
    public Popper popper(@Autowired DAGOperations dagOperations) {
        return new DefaultPopper(dagOperations);
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagTaskDispatcher")
    public DAGDispatcher dagTaskDispatcher() {
        throw new OlympicenceStarterException("need customized DAGDispatcher type bean");
    }

    @Bean
    @ConditionalOnMissingBean(name = "timeChecker")
    public DefaultTimeChecker timeChecker() {
        throw new OlympicenceStarterException("need customized TimeChecker type bean");
    }

    @Bean
    @ConditionalOnMissingBean(name = "runnerExecutor")
    public ExecutorService runnerExecutor() {
        log.info("begin to init default runnerExecutor bean");
        return SameThreadExecutorService.INSTANCE;
    }

    @Bean
    @ConditionalOnMissingBean(name = "traversalExecutor")
    public ExecutorService traversalExecutor() {
        log.info("begin to init default traversalExecutor bean");
        return SameThreadExecutorService.INSTANCE;
    }

    @Bean
    @ConditionalOnMissingBean(name = "notifyExecutor")
    public ExecutorService notifyExecutor() {
        log.info("begin to init default notifyExecutor bean");
        return SameThreadExecutorService.INSTANCE;
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagStringParser")
    public DAGStringParser dagStringParser() {
        log.info("begin to init default DAGStringParser bean");
        return new DAGStringParser(new YAMLSerializer(), Lists.newArrayList(new FlowDAGValidator(), new ResourceDAGValidator()));
    }

    @Bean
    @ConditionalOnMissingBean(name = "inputOutputMapping")
    public JSONPathInputOutputMapping inputOutputMapping() {
        log.info("begin to init default JSONPathInputOutputMapping bean");
        return new JSONPathInputOutputMapping();
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagTraversal")
    public DAGTraversal dagTraversal(
            @Autowired @Qualifier("stasher") Stasher stasher,
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("traversalExecutor") ExecutorService traversalExecutor) {
        log.info("begin to init default DAGTraversal bean");
        DAGTraversal dagTraversal = new DAGTraversal(dagContextStorage, dagInfoStorage, dagStorageProcedure, traversalExecutor);
        dagTraversal.setStasher(stasher);
        return dagTraversal;
    }

    // task runners
    @Bean
    @ConditionalOnMissingBean(name = "functionTaskRunner")
    public FunctionTaskRunner functionTaskRunner(
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired @Qualifier("dagTaskDispatcher") DAGDispatcher dagTaskDispatcher,
            @Autowired SwitcherManager switcherManager) {
        log.info("begin to init default FunctionTaskRunner bean");
        return new FunctionTaskRunner(dagTaskDispatcher, inputOutputMapping, dagContextStorage, dagInfoStorage,
                dagStorageProcedure, switcherManager);
    }

    @Bean
    @ConditionalOnMissingBean(name = "passTaskRunner")
    public PassTaskRunner passTaskRunner(
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired SwitcherManager switcherManager) {
        log.info("begin to init default PassTaskRunner bean");
        return new PassTaskRunner(inputOutputMapping, dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);
    }

    @Bean
    @ConditionalOnMissingBean(name = "suspenseTaskRunner")
    public SuspenseTaskRunner suspenseTaskRunner(
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired SwitcherManager switcherManager) {
        log.info("begin to init default SuspenseTaskRunner bean");
        return new SuspenseTaskRunner(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Bean
    @ConditionalOnMissingBean(name = "returnTaskRunner")
    public ReturnTaskRunner returnTaskRunner(
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired SwitcherManager switcherManager) {
        log.info("begin to init default ReturnTaskRunner bean");
        return new ReturnTaskRunner(inputOutputMapping, dagInfoStorage, dagContextStorage, dagStorageProcedure, switcherManager);
    }

    @Bean
    @ConditionalOnMissingBean(name = "foreachTaskRunner")
    public ForeachTaskRunner foreachTaskRunner(
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired @Qualifier("stasher") Stasher stasher,
            @Autowired SwitcherManager switcherManager) {
        log.info("begin to init default ForeachTaskRunner bean");
        ForeachTaskRunner foreachTaskRunner = new ForeachTaskRunner(inputOutputMapping, inputOutputMapping,
                dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);
        foreachTaskRunner.setStasher(stasher);
        return foreachTaskRunner;
    }

    @Bean
    @ConditionalOnMissingBean(name = "choiceTaskRunner")
    public ChoiceTaskRunner choiceTaskRunner(
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure,
            @Autowired @Qualifier("inputOutputMapping") JSONPathInputOutputMapping inputOutputMapping,
            @Autowired SwitcherManager switcherManager) {
        log.info("begin to init default ChoiceTaskRunner bean");
        return new ChoiceTaskRunner(inputOutputMapping, dagContextStorage, dagInfoStorage, dagStorageProcedure, switcherManager);
    }

    @Bean
    @ConditionalOnMissingBean(name = "taskRunners")
    public Map<String, TaskRunner> taskRunners(
            @Autowired @Qualifier("functionTaskRunner") FunctionTaskRunner functionTaskRunner,
            @Autowired @Qualifier("passTaskRunner") PassTaskRunner passTaskRunner,
            @Autowired @Qualifier("suspenseTaskRunner") SuspenseTaskRunner suspenseTaskRunner,
            @Autowired @Qualifier("returnTaskRunner") ReturnTaskRunner returnTaskRunner,
            @Autowired @Qualifier("foreachTaskRunner") ForeachTaskRunner foreachTaskRunner,
            @Autowired @Qualifier("choiceTaskRunner") ChoiceTaskRunner choiceTaskRunner) {
        log.info("begin to init default Map<TaskCategory, TaskRunner> bean");
        Map<String, TaskRunner> taskRunners = Maps.newConcurrentMap();
        taskRunners.put(TaskCategory.FUNCTION.getValue(), functionTaskRunner);
        taskRunners.put(TaskCategory.PASS.getValue(), passTaskRunner);
        taskRunners.put(TaskCategory.SUSPENSE.getValue(), suspenseTaskRunner);
        taskRunners.put(TaskCategory.RETURN.getValue(), returnTaskRunner);
        taskRunners.put(TaskCategory.FOREACH.getValue(), foreachTaskRunner);
        taskRunners.put(TaskCategory.CHOICE.getValue(), choiceTaskRunner);
        return taskRunners;
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagRunner")
    public DAGRunner dagRunner(
            @Autowired @Qualifier("stasher") Stasher stasher,
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure) {
        log.info("begin to init default DAGRunner bean");
        DAGRunner dagRunner = new DAGRunner(dagContextStorage, dagInfoStorage, dagStorageProcedure);
        dagRunner.setStasher(stasher);
        return dagRunner;
    }

    @Bean
    @ConditionalOnMissingBean(name = "timeCheckRunner")
    public TimeCheckRunner timeCheckRunner(
            @Autowired @Qualifier("timeChecker") TimeChecker timeChecker,
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagContextStorage") DAGContextStorage dagContextStorage,
            @Autowired @Qualifier("dagStorageProcedure") DAGStorageProcedure dagStorageProcedure) {
        log.info("begin to init default TimeCheckRunner bean");
        return new TimeCheckRunner(timeChecker, dagInfoStorage, dagContextStorage, dagStorageProcedure);
    }

    @Bean
    @ConditionalOnMissingBean(name = "dagOperations")
    public DAGOperations dagOperations(
            @Autowired @Qualifier("taskRunners") Map<String, TaskRunner> taskRunners,
            @Autowired @Qualifier("dagRunner") DAGRunner dagRunner,
            @Autowired @Qualifier("dagTraversal") DAGTraversal dagTraversal,
            @Autowired @Qualifier("dagCallback") Callback<DAGCallbackInfo> dagCallback,
            @Autowired @Qualifier("timeCheckRunner") TimeCheckRunner timeCheckRunner,
            @Autowired @Qualifier("runnerExecutor") ExecutorService runnerExecutor,
            @Autowired(required = false) @Qualifier("dagResultHandler") DAGResultHandler dagResultHandler) {
        log.info("begin to init default DAGOperations bean");
        DAGOperations dagOperations = new DAGOperations(runnerExecutor, taskRunners, dagRunner,
                timeCheckRunner, dagTraversal, dagCallback, dagResultHandler);
        dagTraversal.setDagOperations(dagOperations);
        timeCheckRunner.setDagOperations(dagOperations);
        return dagOperations;
    }

    @Bean
    @ConditionalOnMissingBean(name = "olympicene")
    public Olympicene olympicene(
            @Autowired @Qualifier("dagInfoStorage") DAGInfoStorage dagInfoStorage,
            @Autowired @Qualifier("dagOperations") DAGOperations dagOperations,
            @Autowired @Qualifier("notifyExecutor") ExecutorService notifyExecutor,
            @Autowired(required = false) @Qualifier("dagResultHandler") DAGResultHandler dagResultHandler) {
        log.info("begin to init default Olympicene bean");
        return new Olympicene(dagInfoStorage, dagOperations, notifyExecutor, dagResultHandler);
    }
}
