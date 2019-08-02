package com.github.bsideup.liiklus.transport.grpc.config;

import com.github.bsideup.liiklus.service.LiiklusService;
import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class GRPCConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner()
            .withInitializer(context -> {
                ((GenericApplicationContext) context).registerBean(LiiklusService.class, () -> {
                    return Mockito.mock(LiiklusService.class);
                });
            })
            .withInitializer((ApplicationContextInitializer) new GRPCConfiguration());

    @Test
    void shouldRequireGatewayProfile() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(GRPCLiiklusService.class);
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
                "grpc.enabled: false"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(GRPCLiiklusService.class);
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
                "grpc.port: 0"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .hasNotFailed();
        });
    }

}