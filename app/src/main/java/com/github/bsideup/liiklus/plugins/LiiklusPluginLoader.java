package com.github.bsideup.liiklus.plugins;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginLoader;
import org.pf4j.PluginManager;
import org.pf4j.util.FileUtils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

@Slf4j
public class LiiklusPluginLoader implements PluginLoader {

    private final PluginManager pluginManager;

    public LiiklusPluginLoader(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    @Override
    public boolean isApplicable(Path pluginPath) {
        return Files.exists(pluginPath) && FileUtils.isJarFile(pluginPath);
    }

    @Override
    public ClassLoader loadPlugin(Path pluginPath, PluginDescriptor pluginDescriptor) {
        var pluginClassLoader = new PluginClassLoader(pluginManager, pluginDescriptor, Thread.currentThread().getContextClassLoader());
        pluginClassLoader.addFile(pluginPath.toFile());

        // TODO consider fat jars
        try (var jarFileSystem = FileSystems.newFileSystem(pluginPath, null)) {
            var libPath = jarFileSystem.getPath("lib");
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
        } catch (IOException e) {
            throw new RuntimeException("Failed to load JARs from " + pluginPath.toAbsolutePath(), e);
        }

        return pluginClassLoader;
    }
}
