package com.github.bsideup.liiklus.plugins;

import lombok.Getter;
import lombok.NonNull;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ExtensionFinder;
import org.pf4j.ManifestPluginDescriptorFinder;
import org.pf4j.PluginDescriptorFinder;
import org.pf4j.PluginLoader;
import org.pf4j.PluginRepository;
import org.pf4j.ServiceProviderExtensionFinder;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class LiiklusPluginManager extends DefaultPluginManager {

    @Getter
    final String pluginsPathMatcher;

    public LiiklusPluginManager(@NonNull Path pluginsRoot, @NonNull String pluginsPathMatcher) {
        super(pluginsRoot);
        this.pluginsPathMatcher = pluginsPathMatcher;
    }

    @Override
    protected PluginDescriptorFinder createPluginDescriptorFinder() {
        return new ManifestPluginDescriptorFinder();
    }

    @Override
    protected PluginRepository createPluginRepository() {
        return new LiiklusPluginRepository(this);
    }

    @Override
    protected ExtensionFinder createExtensionFinder() {
        return new ServiceProviderExtensionFinder(this) {
            @Override
            public Map<String, Set<String>> readClasspathStorages() {
                // Loads too much (including some javax.servlet.ServletContainerInitializer classes)
                // Since the main jar does not provide any extensions anyways, we can return an empty map here)
                return Collections.emptyMap();
            }
        };
    }

    @Override
    protected PluginLoader createPluginLoader() {
        return new LiiklusPluginLoader(this);
    }
}
