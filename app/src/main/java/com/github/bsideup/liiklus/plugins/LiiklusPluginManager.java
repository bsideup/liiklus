package com.github.bsideup.liiklus.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

@Slf4j
@Getter
public class LiiklusPluginManager extends DefaultPluginManager {

    final String pluginsPathMatcher;

    public LiiklusPluginManager(@NonNull Path pluginsRoot, @NonNull String pluginsPathMatcher) {
        super(pluginsRoot);
        this.pluginsPathMatcher = pluginsPathMatcher;
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
        return new DefaultPluginLoader(this, pluginClasspath) {

            @Override
            protected void loadClasses(Path pluginPath, PluginClassLoader pluginClassLoader) {
                // Don't
            }

            protected void loadJars(Path pluginPath, PluginClassLoader pluginClassLoader) {
                pluginClassLoader.addFile(pluginPath.toFile());
                try (FileSystem jarFileSystem = FileSystems.newFileSystem(pluginPath, null)) {
                    for (String libDirectory : this.pluginClasspath.getLibDirectories()) {
                        Path libPath = jarFileSystem.getPath(libDirectory);
                        if (Files.exists(libPath)) {
                            try (Stream<Path> pathStream = Files.walk(libPath, 1)) {
                                pathStream.filter(it -> !Files.isDirectory(it)).forEach(it -> {
                                    try {
                                        Path tempFile = Files.createTempFile(it.getFileName().toString(), ".jar");
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
        };
    }
}
