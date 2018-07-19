package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.plugins.LiiklusPluginManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.DefaultResourceLoader;

import java.nio.file.*;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class Application {

    static {
        // TODO https://github.com/grpc/grpc-java/issues/4317
        // https://github.com/netty/netty/issues/5930
        System.setProperty("io.netty.recycler.maxCapacity", "0");
        System.setProperty("io.netty.allocator.useCacheForAllThreads", "false");
        System.setProperty("io.netty.allocator.type", "unpooled");
        System.setProperty("io.netty.allocator.numHeapArenas", "0");
        System.setProperty("io.netty.allocator.numDirectArenas", "0");
        System.setProperty("io.netty.allocator.tinyCacheSize", "0");
        System.setProperty("io.netty.allocator.smallCacheSize", "0");
        System.setProperty("io.netty.allocator.normalCacheSize", "0");
    }

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

        Path pluginsRoot = Paths.get(pluginsDir).toAbsolutePath().normalize();
        log.info("Loading plugins from '{}' with matcher: '{}'", pluginsRoot, pathMatcher);

        val pluginManager = new LiiklusPluginManager(pluginsRoot, pathMatcher);

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
