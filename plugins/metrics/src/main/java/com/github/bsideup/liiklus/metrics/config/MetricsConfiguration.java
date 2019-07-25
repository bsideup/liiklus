package com.github.bsideup.liiklus.metrics.config;

import com.github.bsideup.liiklus.metrics.MetricsCollector;
import com.google.auto.service.AutoService;
import io.prometheus.client.exporter.common.TextFormat;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;

@AutoService(ApplicationContextInitializer.class)
public class MetricsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("exporter"))) {
            return;
        }

        applicationContext.registerBean(MetricsCollector.class);

        applicationContext.registerBean("prometheus", RouterFunction.class, () -> {
            var metricsCollector = applicationContext.getBean(MetricsCollector.class);
            return RouterFunctions.route()
                    .GET("/prometheus", __ -> {
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
                    })
                    .build();

        });
    }
}