package com.github.bsideup.liiklus.pulsar.config;

import com.github.bsideup.liiklus.pulsar.config.PulsarRecordsStorageConfiguration.PulsarProperties;
import com.github.bsideup.liiklus.pulsar.container.PulsarTlsContainer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PulsarRecordsStorageConfigurationTest {

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