package com.github.bsideup.liiklus.plugins;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.pf4j.*;
import org.pf4j.processor.ServiceProviderExtensionStorage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class LiiklusExtensionFinder implements ExtensionFinder, PluginStateListener {

    protected final PluginManager pluginManager;

    protected final LoadingCache<String, Map<String, Set<String>>> extensionsCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Map<String, Set<String>>>() {
                @Override
                public Map<String, Set<String>> load(String pluginId) {
                    try {
                        val extensionsResource = ServiceProviderExtensionStorage.EXTENSIONS_RESOURCE;
                        val url = ((PluginClassLoader) pluginManager.getPluginClassLoader(pluginId)).findResource(extensionsResource);

                        if (url != null) {
                            if (url.toURI().getScheme().equals("jar")) {
                                try (val fileSystem = FileSystems.newFileSystem(url.toURI(), Collections.emptyMap())) {
                                    return readExtensions(fileSystem.getPath(extensionsResource));
                                } catch (FileSystemAlreadyExistsException e) {
                                    try (val fileSystem = FileSystems.getFileSystem(url.toURI())) {
                                        return readExtensions(fileSystem.getPath(extensionsResource));
                                    }
                                }
                            } else {
                                return readExtensions(Paths.get(url.toURI()));
                            }
                        }
                    } catch (IOException | URISyntaxException e) {
                        log.error(e.getMessage(), e);
                    }

                    return Collections.emptyMap();
                }

                private Map<String, Set<String>> readExtensions(Path extensionPath) throws IOException {
                    val result = new HashMap<String, Set<String>>();

                    Files.walkFileTree(extensionPath, Collections.emptySet(), 1, new SimpleFileVisitor<Path>() {

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            val serviceClassName = file.getFileName().toString();
                            try (val reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                                val implementations = new HashSet<String>();
                                ServiceProviderExtensionStorage.read(reader, implementations);

                                result.put(serviceClassName, implementations);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });

                    return result;
                }
            });

    public LiiklusExtensionFinder(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
        pluginManager.addPluginStateListener(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<ExtensionWrapper<T>> find(Class<T> type) {
        return pluginManager.getPlugins().stream()
                .map(it -> it.getDescriptor().getPluginId())
                .flatMap(it -> find(type, it).stream())
                .sorted()
                .collect(Collectors.toList());
    }

    @Override
    public <T> List<ExtensionWrapper<T>> find(Class<T> type, String pluginId) {
        return findExtensions(pluginId, type);
    }

    @Override
    public List<ExtensionWrapper> find(String pluginId) {
        return (List) findExtensions(pluginId, null);
    }

    protected <T> List<ExtensionWrapper<T>> findExtensions(String pluginId, Class<T> type) {
        val extensions = extensionsCache.getUnchecked(pluginId);
        if (extensions.isEmpty()) {
            return Collections.emptyList();
        }

        val pluginWrapper = pluginManager.getPlugin(pluginId);
        if (PluginState.STARTED != pluginWrapper.getPluginState()) {
            return Collections.emptyList();
        }

        val classLoader = pluginManager.getPluginClassLoader(pluginId);

        Stream<String> classNames;

        if (type == null) {
            classNames = extensions.values().stream().flatMap(Collection::stream);
        } else {
            classNames = extensions.getOrDefault(type.getName(), Collections.emptySet()).stream();
        }

        return classNames
                .flatMap(className -> {
                    try {
                        val extensionClass = classLoader.loadClass(className);

                        int ordinal = 0;
                        if (extensionClass.isAnnotationPresent(Extension.class)) {
                            ordinal = extensionClass.getAnnotation(Extension.class).ordinal();
                        }
                        val descriptor = new ExtensionDescriptor(ordinal, extensionClass);

                        val extensionWrapper = new ExtensionWrapper<T>(descriptor, pluginManager.getExtensionFactory());
                        return Stream.of(extensionWrapper);
                    } catch (ClassNotFoundException e) {
                        log.error(e.getMessage(), e);
                    }
                    return Stream.empty();
                })
                .sorted()
                .collect(Collectors.toList());
    }

    @Override
    public Set<String> findClassNames(String pluginId) {
        return extensionsCache.getUnchecked(pluginId)
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void pluginStateChanged(PluginStateEvent event) {
        // TODO optimize (do only for some transitions)
        extensionsCache.invalidate(event.getPlugin().getPluginId());
    }
}
