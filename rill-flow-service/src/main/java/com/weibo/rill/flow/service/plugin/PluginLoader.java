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
