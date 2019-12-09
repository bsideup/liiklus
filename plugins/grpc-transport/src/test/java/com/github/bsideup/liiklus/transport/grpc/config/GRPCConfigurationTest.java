package com.github.bsideup.liiklus.transport.grpc.config;

import com.github.bsideup.liiklus.service.LiiklusService;
import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusService;
import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusTransportConfigurer;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class GRPCConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
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

    @Test
    void shouldConsiderTransportConfigurers() {
        var service = ServerServiceDefinition.builder("test").build();

        new ApplicationContextRunner()
                .withInitializer((ApplicationContextInitializer) new GRPCConfiguration())
                .withPropertyValues(
                        "spring.profiles.active: gateway",
                        "grpc.port: 0"
                )
                .withInitializer(ctx -> {
                    var context = (GenericApplicationContext) ctx;
                    context.registerBean(LiiklusService.class, () -> Mockito.mock(LiiklusService.class));
                    context.registerBean(GRPCLiiklusTransportConfigurer.class, () -> builder -> builder.addService(() -> service));
                })
                .run(context -> {
                    assertThat(context).getBeans(GRPCLiiklusTransportConfigurer.class).isNotEmpty();

                    assertThat(context)
                            .getBean(Server.class)
                            .satisfies(server -> {
                                assertThat(server.getServices()).contains(service);
                            });
                });
    }
}