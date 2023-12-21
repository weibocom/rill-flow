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

package com.weibo.rill.flow.service.plugin;

import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.service.dispatcher.FunctionTaskDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.JarPluginManager;
import org.pf4j.PluginManager;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class PluginLoader {

    @PostConstruct
    public void postConstruct() {
        doBeanCreate();
    }

    public static final Set<DispatcherExtension> TASK_EXTENSION_SET = new HashSet<>();

    public void doBeanCreate() {
        String pluginsBaseDirectory = "/usr/local/rill_flow/plugins";
        System.setProperty("pf4j.pluginsDir", pluginsBaseDirectory);
        try {
            registerDispatcherBeanFromJar();
            log.info("plugin load success: {}", "aliyun_ai");
        } catch (Exception e) {
            log.warn("load plugin error: ", e);
        }
    }

    private void registerDispatcherBeanFromJar() {
        PluginManager pluginManager = new JarPluginManager();
        pluginManager.loadPlugins();
        pluginManager.startPlugins();

        List<DispatcherExtension> dispatcherExtensions = pluginManager.getExtensions(DispatcherExtension.class);
        for (DispatcherExtension dispatcherExtension : dispatcherExtensions) {
            TASK_EXTENSION_SET.add(dispatcherExtension);
            FunctionTaskDispatcher.protocolDispatcherMap.put(dispatcherExtension.getName(), dispatcherExtension);
        }
    }
}
