package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.config.GatewayConfiguration;
import com.github.bsideup.liiklus.plugins.LiiklusPluginManager;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.fu.jafu.Jafu;
import org.springframework.fu.jafu.webflux.WebFluxServerDsl;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.nio.file.Paths;

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

    @SafeVarargs
    public static ConfigurableApplicationContext start(
            String[] args,
            ApplicationContextInitializer<GenericApplicationContext>... additionalInitializers
    ) {
        return Jafu.reactiveWebApplication(app -> {
            var environment = app.env();

            var pluginsDir = environment.getProperty("plugins.dir", String.class, "./plugins");
            var pathMatcher = environment.getProperty("plugins.pathMatcher", String.class, "*.jar");

            var pluginsRoot = Paths.get(pluginsDir).toAbsolutePath().normalize();
            log.info("Loading plugins from '{}' with matcher: '{}'", pluginsRoot, pathMatcher);

            var pluginManager = new LiiklusPluginManager(pluginsRoot, pathMatcher);

            pluginManager.loadPlugins();
            pluginManager.startPlugins();

            app.enable(new GatewayConfiguration());
            app.beans(beans -> {
                beans.bean("health", RouterFunction.class, () -> {
                    return RouterFunctions.route()
                            .GET("/health", __ -> ServerResponse.ok().bodyValue("OK"))
                            .build();
                });
                beans.bean(PluginManager.class, () -> pluginManager);
            });

            for (var initializerClass : pluginManager.getExtensionClasses(ApplicationContextInitializer.class)) {
                try {
                    app.enable(initializerClass.getDeclaredConstructor().newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            for (var initializer : additionalInitializers) {
                app.enable(initializer);
            }

            app.enable(WebFluxServerDsl.webFlux(dsl -> {
                try {
                    var serverPropertiesField = WebFluxServerDsl.class.getDeclaredField("serverProperties");
                    serverPropertiesField.setAccessible(true);
                    serverPropertiesField.set(dsl, Binder.get(app.env()).bindOrCreate("server", ServerProperties.class));
                } catch (IllegalAccessException | NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }));
        }).run(args);
    }
}
