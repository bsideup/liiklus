package com.github.bsideup.liiklus.transport.rsocket.config;

import com.github.bsideup.liiklus.transport.rsocket.RSocketLiiklusService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class RSocketConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new RSocketConfiguration());

    @Test
    void shouldRequireGatewayProfile() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(RSocketLiiklusService.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
    }

    @Test
    void shouldBeDisableable() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "rsocket.enabled: false"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(RSocketLiiklusService.class);
        });
    }

    @Test
    void shouldValidateParameters() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "rsocket.host: localhost"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "rsocket.port: 0"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .hasNotFailed();
        });
    }
}