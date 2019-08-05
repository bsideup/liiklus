package com.github.bsideup.liiklus.schema;

import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaPluginConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new SchemaPluginConfiguration());

    @Test
    void shouldNotBeEnabledByDefault() {
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(JsonSchemaPreProcessor.class);
        });
    }

    @Test
    void shouldRequireGatewayProfile() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "schema.enabled: true"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(JsonSchemaPreProcessor.class);
        });
    }

    @Test
    void shouldValidateProperties() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "schema.enabled: true"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .isInstanceOf(BindException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "schema.schemaURL: " + getSchemaURL()
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasNotFailed();
        });
    }

    private static URL getSchemaURL() {
        return Thread.currentThread().getContextClassLoader().getResource("schemas/basic.yml");
    }
}