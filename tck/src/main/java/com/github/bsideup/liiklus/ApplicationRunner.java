package com.github.bsideup.liiklus;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.loader.JarLauncher;
import org.springframework.boot.loader.LaunchedURLClassLoader;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;

@RequiredArgsConstructor
public class ApplicationRunner {

    @NonNull
    final String recordsStorageType;

    @NonNull
    final String positionsStorageType;

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
            application.setDefaultProperties(Map.of(
                    "server.port", 0,
                    "rsocket.enabled", false,
                    "grpc.enabled", false,
                    "storage.records.type", recordsStorageType,
                    "storage.positions.type", positionsStorageType
            ));
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
