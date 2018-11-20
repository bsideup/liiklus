package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.config.GRPCConfiguration;
import com.github.bsideup.liiklus.config.LayersConfiguration;
import com.github.bsideup.liiklus.config.MetricsConfiguration;
import com.github.bsideup.liiklus.config.RSocketConfiguration;
import com.github.bsideup.liiklus.monitoring.MetricsCollector;
import com.github.bsideup.liiklus.plugins.LiiklusPluginManager;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ResourceProperties;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.autoconfigure.web.reactive.ReactiveWebServerInitializer;
import org.springframework.boot.autoconfigure.web.reactive.ResourceCodecInitializer;
import org.springframework.boot.autoconfigure.web.reactive.StringCodecInitializer;
import org.springframework.boot.autoconfigure.web.reactive.WebFluxProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.function.server.support.RouterFunctionMapping;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.Collections;

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
        var environment = new StandardEnvironment();
        environment.setDefaultProfiles("exporter", "gateway");
        environment.getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));

        var pluginsDir = environment.getProperty("plugins.dir", String.class, "./plugins");
        var pathMatcher = environment.getProperty("plugins.pathMatcher", String.class, "*.jar");

        var pluginsRoot = Paths.get(pluginsDir).toAbsolutePath().normalize();
        log.info("Loading plugins from '{}' with matcher: '{}'", pluginsRoot, pathMatcher);

        var pluginManager = new LiiklusPluginManager(pluginsRoot, pathMatcher);

        pluginManager.loadPlugins();
        pluginManager.startPlugins();

        var binder = Binder.get(environment);
        var application = new SpringApplication(Application.class) {
            @Override
            protected void load(ApplicationContext context, Object[] sources) {
                // We don't want the annotation bean definition reader
            }
        };
        application.setWebApplicationType(WebApplicationType.REACTIVE);
        application.setApplicationContextClass(ReactiveWebServerApplicationContext.class);
        application.setEnvironment(environment);

        application.addInitializers(
                new StringCodecInitializer(false),
                new ResourceCodecInitializer(false),
                new ReactiveWebServerInitializer(
                        binder.bind("server", ServerProperties.class).orElseGet(ServerProperties::new),
                        binder.bind("spring.resources", ResourceProperties.class).orElseGet(ResourceProperties::new),
                        binder.bind("spring.webflux", WebFluxProperties.class).orElseGet(WebFluxProperties::new),
                        new NettyReactiveWebServerFactory()
                ),
                new GRPCConfiguration(),
                new RSocketConfiguration(),
                new LayersConfiguration(),
                new MetricsConfiguration(),
                (GenericApplicationContext applicationContext) -> {
                    applicationContext.registerBean(RouterFunctionMapping.class, () -> {
                        var router = RouterFunctions.route();
                        router.GET("/health", __ -> ServerResponse.ok().syncBody("OK"));

                        if (environment.acceptsProfiles(Profiles.of("exporter"))) {
                            var metricsCollector = applicationContext.getBean(MetricsCollector.class);
                            router.GET("/prometheus", __ -> {
                                return metricsCollector.collect()
                                        .collectList()
                                        .flatMap(metrics -> {
                                            try {
                                                var writer = new StringWriter();
                                                TextFormat.write004(writer, Collections.enumeration(metrics));
                                                return ServerResponse.ok()
                                                        .contentType(MediaType.valueOf(TextFormat.CONTENT_TYPE_004))
                                                        .syncBody(writer.toString());
                                            } catch (IOException e) {
                                                return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                                            }
                                        });
                            });
                        }
                        return new RouterFunctionMapping(router.build());
                    });
                }
        );

        application.addInitializers(
                pluginManager.getExtensionClasses(ApplicationContextInitializer.class).stream()
                        .map(it -> {
                            try {
                                return it.newInstance();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .toArray(ApplicationContextInitializer[]::new)
        );

        return application;
    }
}
