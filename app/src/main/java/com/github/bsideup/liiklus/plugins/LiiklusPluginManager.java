package com.github.bsideup.liiklus.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Delegate;
import org.pf4j.*;

import java.nio.file.Path;

public class LiiklusPluginManager extends DefaultPluginManager {

    @Getter
    final String pluginsPathMatcher;

    public LiiklusPluginManager(@NonNull Path pluginsRoot, @NonNull String pluginsPathMatcher) {
        super(pluginsRoot);
        this.pluginsPathMatcher = pluginsPathMatcher;
    }

    @Override
    protected VersionManager createVersionManager() {
        var versionManager = super.createVersionManager();

        class DelegatingVersionManager implements VersionManager {
            @Delegate
            final VersionManager delegate = versionManager;
        }

        return new DelegatingVersionManager() {
            @Override
            public boolean checkVersionConstraint(String version, String constraint) {
                // TODO https://github.com/pf4j/pf4j/issues/367
                return "*".equals(constraint) || super.checkVersionConstraint(version, constraint);
            }
        };
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
