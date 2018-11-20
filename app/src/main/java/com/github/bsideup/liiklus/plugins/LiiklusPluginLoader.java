package com.github.bsideup.liiklus.plugins;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.DefaultPluginLoader;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginClasspath;
import org.pf4j.PluginManager;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

@Slf4j
public class LiiklusPluginLoader extends DefaultPluginLoader {

    public LiiklusPluginLoader(PluginManager pluginManager, PluginClasspath pluginClasspath) {
        super(pluginManager, pluginClasspath);
    }

    @Override
    protected void loadClasses(Path pluginPath, PluginClassLoader pluginClassLoader) {
        // Don't load classes, but add plugin's JAR instead
        pluginClassLoader.addFile(pluginPath.toFile());
    }

    @Override
    protected void loadJars(Path pluginPath, PluginClassLoader pluginClassLoader) {
        try (var jarFileSystem = FileSystems.newFileSystem(pluginPath, null)) {
            for (var libDirectory : this.pluginClasspath.getLibDirectories()) {
                var libPath = jarFileSystem.getPath(libDirectory);
                if (Files.exists(libPath)) {
                    try (var pathStream = Files.walk(libPath, 1)) {
                        pathStream.filter(Files::isRegularFile).forEach(it -> {
                            try {
                                var tempFile = Files.createTempFile(it.getFileName().toString(), ".jar");
                                Files.copy(it, tempFile, StandardCopyOption.REPLACE_EXISTING);
                                pluginClassLoader.addURL(tempFile.toUri().toURL());
                            } catch (Exception e) {
                                log.error("Failed to add file from {}", it.toAbsolutePath(), e);
                            }
                        });
                    }
                }
            }
        } catch (IOException e) {
            log.error("Failed to load JARs from {}", pluginPath.toAbsolutePath(), e);
        }
    }
}
