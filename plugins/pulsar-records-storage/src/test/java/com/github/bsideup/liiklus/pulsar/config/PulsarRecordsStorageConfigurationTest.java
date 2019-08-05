package com.github.bsideup.liiklus.pulsar.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.pulsar.PulsarRecordsStorage;
import com.github.bsideup.liiklus.pulsar.config.PulsarRecordsStorageConfiguration.PulsarProperties;
import com.github.bsideup.liiklus.pulsar.container.PulsarTlsContainer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;
import org.testcontainers.utility.MountableFile;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PulsarRecordsStorageConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new PulsarRecordsStorageConfiguration());

    @Test
    void shouldSkipWhenNotPulsar() {
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldSkipIfNotGateway() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "storage.records.type: PULSAR"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldValidateProperties() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "storage.records.type: PULSAR"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "pulsar.serviceUrl: host:6650"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(PulsarRecordsStorage.class);
        });
    }

    @Test
    void shouldRegisterWhenPulsar() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "storage.records.type: PULSAR",
                "pulsar.serviceUrl: host:6650"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(PulsarRecordsStorage.class);
        });
    }

    @Test
    void shouldSupportTLS() throws Exception {
        try (var pulsar = new PulsarTlsContainer()) {
            pulsar.start();
            var configuration = new PulsarRecordsStorageConfiguration();

            var properties = new PulsarProperties();
            properties.setServiceUrl(pulsar.getPulsarBrokerUrl());

            var topicName = UUID.randomUUID().toString();

            try (var client = configuration.createClient(properties)) {
                assertThatThrownBy(() -> client.getPartitionsForTopic(topicName).get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(PulsarClientException.class);
            }

            properties.setTlsTrustCertsFilePath(Optional.of(pulsar.getCaCert().toAbsolutePath().toString()));

            try (var client = configuration.createClient(properties)) {
                assertThat(client.getPartitionsForTopic(topicName).get(10, TimeUnit.SECONDS))
                        .isNotEmpty();
            }
        }
    }

    @Test
    void shouldSupportAuth() throws Exception {
        try (var pulsar = new PulsarTlsContainer()) {
            pulsar.withCopyFileToContainer(MountableFile.forClasspathResource(".htpasswd"), "/pulsar/conf/.htpasswd");
            pulsar.withEnv("PULSAR_MEM", "-Dpulsar.auth.basic.conf=/pulsar/conf/.htpasswd");
            pulsar.withEnv("superUserRoles", "super");

            pulsar.withEnv("authenticationEnabled", "true");
            pulsar.withEnv("authenticationProviders", "org.apache.pulsar.broker.authentication.AuthenticationProviderBasic");

            pulsar.withEnv("brokerClientAuthenticationPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationBasic");
            pulsar.withEnv("brokerClientAuthenticationParameters", "{\"userId\":\"super\",\"password\":\"superpass\"}");
            pulsar.start();
            var configuration = new PulsarRecordsStorageConfiguration();

            var topicName = UUID.randomUUID().toString();

            var properties = new PulsarProperties();
            properties.setServiceUrl(pulsar.getPulsarBrokerUrl());
            properties.setTlsTrustCertsFilePath(Optional.of(pulsar.getCaCert().toAbsolutePath().toString()));

            try (var client = configuration.createClient(properties)) {
                assertThatThrownBy(() -> client.getPartitionsForTopic(topicName).get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(PulsarClientException.class);
            }

            properties.setAuthPluginClassName(Optional.of(AuthenticationBasic.class.getName()));
            properties.setAuthPluginParams(Map.of(
                    "userId", "super",
                    "password", UUID.randomUUID().toString()
            ));

            try (var client = configuration.createClient(properties)) {
                assertThatThrownBy(() -> client.getPartitionsForTopic(topicName).get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(PulsarClientException.class);
            }

            properties.setAuthPluginParams(Map.of(
                    "userId", "super",
                    "password", "superpass"
            ));

            try (var client = configuration.createClient(properties)) {
                assertThat(client.getPartitionsForTopic(topicName).get(10, TimeUnit.SECONDS))
                        .isNotEmpty();
            }
        }
    }
}