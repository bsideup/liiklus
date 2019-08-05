package com.github.bsideup.liiklus.metrics.config;

import com.github.bsideup.liiklus.metrics.MetricsCollector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new MetricsConfiguration());

    @Test
    void shouldNotBeEnabledByDefault() {
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(MetricsCollector.class);
        });
    }

    @Test
    void shouldRequireExporterProfile() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_exporter"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(MetricsCollector.class);
        });
    }

    @Test
    void shouldRegisterWhenExporter() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: exporter"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(MetricsCollector.class);
        });
    }
}