package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.monitoring.MetricsCollector;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;

public class MetricsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("exporter"))) {
            return;
        }

        applicationContext.registerBean(MetricsCollector.class);
    }
}