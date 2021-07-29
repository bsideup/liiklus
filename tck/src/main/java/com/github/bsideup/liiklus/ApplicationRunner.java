package com.github.bsideup.liiklus;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.boot.loader.JarLauncher;
import org.springframework.boot.loader.LaunchedURLClassLoader;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class ApplicationRunner implements BeforeTestExecutionCallback, ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(ApplicationRunner.class);
    private static final String KEY = "ctx";

    final Map<String, Supplier<Object>> properties = new HashMap<>(Map.of(
            "server.port", () -> 0,
            "rsocket.enabled", () -> false,
            "grpc.enabled", () -> false,
            "plugins.dir", () -> findPluginsDir().getAbsolutePath(),
            "plugins.pathMatcher", () -> "*/build/libs/*.jar"
    ));

    public ApplicationRunner(@NonNull String recordsStorageType, @NonNull String positionsStorageType) {
        withProperty("storage.records.type", recordsStorageType);
        withProperty("storage.positions.type", positionsStorageType);
    }

    public ApplicationRunner withProperty(String key, Object value) {
        return withProperty(key, () -> value);
    }

    public ApplicationRunner withProperty(String key, Supplier<Object> value) {
        properties.put(key, value);
        return this;
    }

    @SneakyThrows
    public ConfigurableApplicationContext run() {
        var tempFile = Files.createTempFile("app", ".jar");
        tempFile.toFile().deleteOnExit();
        try (var appJarStream = getClass().getClassLoader().getResourceAsStream("app-boot.jar")) {
            Files.copy(appJarStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }

        var launcher = new JarLauncher(new JarFileArchive(tempFile.toFile(), tempFile.toUri().toURL())) {

            ClassLoader createClassLoader() throws Exception {
                return super.createClassLoader(getClassPathArchives());
            }

            @Override
            protected ClassLoader createClassLoader(URL[] urls) throws Exception {
                var systemClassLoader = ClassLoader.getSystemClassLoader();
                return new LaunchedURLClassLoader(urls, systemClassLoader.getParent()) {

                    @Override
                    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                        var classFile = findResource(name.replace(".", "/") + ".class");
                        if (classFile != null) {
                            // If exists in the app.jar, load it from the system classloader instead
                            log.debug("Loading class '{}' from the system ClassLoader instead", name);
                            return systemClassLoader.loadClass(name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };
            }
        };

        var currentClassLoader = Thread.currentThread().getContextClassLoader();
        var oldProperties = new Properties(System.getProperties());

        try {
            var appClassLoader = launcher.createClassLoader();
            Thread.currentThread().setContextClassLoader(appClassLoader);

            System.getProperties().putAll(properties.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get())));

            var applicationClass = appClassLoader.loadClass("com.github.bsideup.liiklus.Application");

            var createSpringApplicationMethod = applicationClass.getDeclaredMethod(
                    "start",
                    String[].class,
                    ApplicationContextInitializer[].class
            );
            return (ConfigurableApplicationContext) createSpringApplicationMethod.invoke(
                    null,
                    new String[0],
                    new ApplicationContextInitializer[0]
            );
        } finally {
            System.setProperties(oldProperties);
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        var ctx = run();
        context.getStore(NAMESPACE).put(KEY, ctx);
        context.getStore(NAMESPACE).put("closeable", (ExtensionContext.Store.CloseableResource) ctx::close);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return ConfigurableApplicationContext.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public ConfigurableApplicationContext resolveParameter(ParameterContext parameterContext, ExtensionContext context) throws ParameterResolutionException {
        return (ConfigurableApplicationContext) context.getStore(NAMESPACE).get(KEY);
    }

    public static File findPluginsDir() {
        var cwd = new File(".");
        var projectDir = cwd.getAbsoluteFile();
        File pluginsDir;
        do {
            pluginsDir = new File(projectDir, "plugins");
            if (pluginsDir.exists()) {
                return pluginsDir;
            }
            projectDir = projectDir.getParentFile();
        } while (projectDir != null);

        throw new IllegalStateException("Failed to find the plugins directory, current dir: " + cwd);
    }
}
