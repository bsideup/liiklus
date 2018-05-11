package com.github.bsideup.liiklus.plugins;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.pf4j.AbstractExtensionFinder;
import org.pf4j.PluginManager;
import org.pf4j.PluginWrapper;
import org.pf4j.processor.ServiceProviderExtensionStorage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

@Slf4j
public class LiiklusExtensionFinder extends AbstractExtensionFinder {

    public LiiklusExtensionFinder(PluginManager pluginManager) {
        super(pluginManager);
    }

    @Override
    public Map<String, Set<String>> readClasspathStorages() {
        return emptyMap();
    }

    @Override
    public Map<String, Set<String>> readPluginsStorages() {
        return pluginManager.getPlugins().stream().collect(Collectors.toMap(PluginWrapper::getPluginId, plugin -> {
            val result = new HashSet<String>();
            try (val fileSystem = FileSystems.newFileSystem(plugin.getPluginPath(), null)) {
                val services = fileSystem.getPath(ServiceProviderExtensionStorage.EXTENSIONS_RESOURCE);
                Files.walkFileTree(services, Collections.emptySet(), 1, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        try (val reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                            ServiceProviderExtensionStorage.read(reader, result);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                log.error("Failed to read extensions of plugin: {}", plugin.getPluginId(), e);
            }
            return result;
        }));
    }
}
