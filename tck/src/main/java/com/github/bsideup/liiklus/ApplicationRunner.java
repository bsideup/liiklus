package com.github.bsideup.liiklus;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.loader.JarLauncher;
import org.springframework.boot.loader.LaunchedURLClassLoader;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ApplicationRunner {

    final Map<String, Object> properties = new HashMap<>(Map.of(
            "server.port", 0,
            "rsocket.enabled", false,
            "grpc.enabled", false
    ));

    public ApplicationRunner(@NonNull String recordsStorageType, @NonNull String positionsStorageType) {
        withProperty("storage.records.type", recordsStorageType);
        withProperty("storage.positions.type", positionsStorageType);
    }

    public ApplicationRunner withProperty(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    @SneakyThrows
    public ConfigurableApplicationContext run() {
        System.setProperty("plugins.dir", findPluginsDir().getAbsolutePath());
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

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
        try {
            var appClassLoader = launcher.createClassLoader();
            Thread.currentThread().setContextClassLoader(appClassLoader);

            var applicationClass = appClassLoader.loadClass("com.github.bsideup.liiklus.Application");

            var createSpringApplicationMethod = applicationClass.getDeclaredMethod("createSpringApplication", String[].class);

            var application = (SpringApplication) createSpringApplicationMethod.invoke(null, (Object) new String[0]);
            application.setDefaultProperties(properties);
            return application.run();
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }

    public File findPluginsDir() {
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
