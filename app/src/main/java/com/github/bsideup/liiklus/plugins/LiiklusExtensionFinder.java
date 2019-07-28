package com.github.bsideup.liiklus.plugins;

import org.pf4j.PluginManager;
import org.pf4j.ServiceProviderExtensionFinder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class LiiklusExtensionFinder extends ServiceProviderExtensionFinder {

    LiiklusExtensionFinder(PluginManager pluginManager) {
        super(pluginManager);
    }

    @Override
    public Map<String, Set<String>> readClasspathStorages() {
        // The app does not provide any extensions,
        // we can safely return an empty Map here to avoid an exception ('META-INF/services' not found)
        return Collections.emptyMap();
    }

    @Override
    public Set<String> findClassNames(String pluginId) {
        var pluginClassLoader = pluginId != null
                ? pluginManager.getPluginClassLoader(pluginId)
                : getClass().getClassLoader();

        return super.findClassNames(pluginId)
                .stream()
                // We need to filter out extension definitions for classes that are not on classpath (e.g. javax CDI)
                .filter(it -> {
                    try {
                        // TODO avoid classloading
                        pluginClassLoader.loadClass(it);
                        return true;
                    } catch (ClassNotFoundException | NoClassDefFoundError e) {
                        return false;
                    }
                })
                .collect(Collectors.toSet());
    }
}
