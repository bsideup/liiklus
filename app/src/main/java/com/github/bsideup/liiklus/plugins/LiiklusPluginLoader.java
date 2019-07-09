package com.github.bsideup.liiklus.plugins;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.DefaultPluginLoader;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginManager;

import java.nio.file.Path;

@Slf4j
public class LiiklusPluginLoader extends DefaultPluginLoader {

    public LiiklusPluginLoader(PluginManager pluginManager) {
        super(pluginManager);
    }

    @Override
    protected void loadClasses(Path pluginPath, PluginClassLoader pluginClassLoader) {
        // Don't load classes, but add plugin's JAR instead
        pluginClassLoader.addFile(pluginPath.toFile());
    }
}
