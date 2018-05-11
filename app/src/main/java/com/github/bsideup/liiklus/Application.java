package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.plugins.LiiklusExtensionFinder;
import com.github.bsideup.liiklus.plugins.LiiklusPluginRepository;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.pf4j.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.DefaultResourceLoader;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class Application {

    public static void main(String[] args) throws Exception {
        start(args);
    }

    public static ConfigurableApplicationContext start(String[] args) {
        return createSpringApplication(args).run(args);
    }

    public static SpringApplication createSpringApplication(String[] args) {
        val environment = new StandardEnvironment();
        environment.setDefaultProfiles("exporter", "gateway");
        environment.getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));

        val pluginsDir = environment.getProperty("plugins.dir", String.class, "./plugins");
        val pathMatcher = environment.getProperty("plugins.pathMatcher", String.class, "*.jar");

        log.info("Loading plugins from {} with matcher: {}", pluginsDir, pathMatcher);

        val pluginManager = new DefaultPluginManager(Paths.get(pluginsDir).toAbsolutePath().normalize()) {

            @Override
            protected PluginRepository createPluginRepository() {
                return new LiiklusPluginRepository(getPluginsRoot(), pathMatcher);
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
                        super.loadJars(pluginPath, pluginClassLoader);
                        try (FileSystem jarFileSystem = FileSystems.newFileSystem(pluginPath, null)) {
                            for (String libDirectory : this.pluginClasspath.getLibDirectories()) {
                                Path libPath = jarFileSystem.getPath(libDirectory);
                                if (Files.exists(libPath)) {
                                    Files.walk(libPath, 1).filter(it -> !Files.isDirectory(it)).forEach(it -> {
                                        try {
                                            Path tempFile = Files.createTempFile(it.getFileName().toString(), ".jar");
                                            Files.copy(it, tempFile, StandardCopyOption.REPLACE_EXISTING);
                                            pluginClassLoader.addURL(tempFile.toUri().toURL());
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    });
                                }
                            }
                        } catch (IOException e) {
                            log.error("", e);
                        }
                    }
                };
            }
        };

        pluginManager.loadPlugins();
        pluginManager.startPlugins();

        val application = new SpringApplication(
                new DefaultResourceLoader() {
                    @Override
                    public ClassLoader getClassLoader() {
                        return new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                            @Override
                            protected Class<?> findClass(String name) throws ClassNotFoundException {
                                try {
                                    return super.findClass(name);
                                } catch (ClassNotFoundException e) {
                                    // FIXME X_X
                                    for (val pluginWrapper : pluginManager.getResolvedPlugins()) {
                                        try {
                                            return pluginWrapper.getPluginClassLoader().loadClass(name);
                                        } catch (ClassNotFoundException __) {
                                            continue;
                                        }
                                    }

                                    throw e;
                                }
                            }
                        };
                    }
                },
                Stream
                        .concat(
                                pluginManager.getExtensionClasses(LiiklusConfiguration.class).stream(),
                                Stream.of(Application.class)
                        )
                        .toArray(Class[]::new)
        );
        application.setEnvironment(environment);

        return application;
    }
}
