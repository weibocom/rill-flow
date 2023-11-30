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

package com.weibo.rill.flow.service.component;

import com.weibo.rill.flow.service.dconfs.BizDConfs;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.service.util.ExecutionIdUtil;
import com.weibo.rill.flow.olympicene.core.concurrent.ExecutionRunnable;
import com.weibo.rill.flow.common.concurrent.BaseExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

@Slf4j
public class RuntimeExecutorServiceProxy extends BaseExecutorService {
    @Autowired
    private SwitcherManager switcherManagerImpl;

    private BizDConfs bizDConfs;
    private final Map<String, ExecutorService> executorsHolder;
    private final ExecutorService bizDefaultExecutorService;


    public RuntimeExecutorServiceProxy(BizDConfs bizDConfs,
                                       Map<String, ExecutorService> executorsHolder,
                                       ExecutorService defaultExecutorService) {
        this.bizDConfs = bizDConfs;
        this.executorsHolder = executorsHolder;
        this.bizDefaultExecutorService = defaultExecutorService;
    }

    @Override
    public void execute(Runnable command) {
        ExecutorService executorService = Optional.ofNullable(command)
                .filter(it -> switcherManagerImpl.getSwitcherState("ENABLE_THREAD_ISOLATION"))
                .filter(ExecutionRunnable.class::isInstance)
                .map(ExecutionRunnable.class::cast)
                .map(ExecutionRunnable::getExecutionId)
                .map(this::chooseByConfiguredBiz)
                .orElse(bizDefaultExecutorService);

        executorService.execute(command);
    }

    private ExecutorService chooseByConfiguredBiz(String executionId) {
        // choose by biz, rule is same as RuntimeRedisClients.choose
        String serviceId = ExecutionIdUtil.getServiceId(executionId);
        String clientId = bizDConfs.getRedisServiceIdToClientId().get(serviceId);
        if (StringUtils.isBlank(clientId)) {
            String businessId = ExecutionIdUtil.getBusinessId(executionId);
            clientId = bizDConfs.getRedisBusinessIdToClientId().get(businessId);
        }
        log.debug("choose by biz and type:{}, clientId:{}", executionId, clientId);

        if (StringUtils.isBlank(clientId)) {
            return bizDefaultExecutorService;
        }

        ExecutorService chosen = this.executorsHolder.get(clientId);
        if (chosen == null) {
            log.warn("choose by biz and type:{} not found in config, may use default.", clientId);
        }

        return chosen;
    }
}
