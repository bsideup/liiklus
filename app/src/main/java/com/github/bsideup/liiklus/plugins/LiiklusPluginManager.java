package com.github.bsideup.liiklus.plugins;

import lombok.Getter;
import lombok.NonNull;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ExtensionFinder;
import org.pf4j.ManifestPluginDescriptorFinder;
import org.pf4j.PluginDescriptorFinder;
import org.pf4j.PluginLoader;
import org.pf4j.PluginRepository;

import java.nio.file.Path;

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
        return new LiiklusExtensionFinder(this);
    }

    @Override
    protected PluginLoader createPluginLoader() {
        return new LiiklusPluginLoader(this);
    }

}
