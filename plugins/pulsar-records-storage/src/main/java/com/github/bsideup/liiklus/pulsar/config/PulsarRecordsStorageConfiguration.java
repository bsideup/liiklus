package com.github.bsideup.liiklus.pulsar.config;

import com.github.bsideup.liiklus.pulsar.PulsarFiniteRecordsStorage;
import com.github.bsideup.liiklus.pulsar.PulsarRecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import java.util.Map;
import java.util.Optional;

@AutoService(ApplicationContextInitializer.class)
public class PulsarRecordsStorageConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        if (!"PULSAR".equals(environment.getProperty("storage.records.type"))) {
            return;
        }

        var binder = Binder.get(environment);

        var pulsarProperties = binder.bind("pulsar", PulsarProperties.class).get();

        pulsarProperties.getAdminUrl().ifPresentOrElse(
                adminUrl -> {
                    applicationContext.registerBean(PulsarFiniteRecordsStorage.class, () -> {
                        try {
                            return new PulsarFiniteRecordsStorage(
                                    createClient(pulsarProperties),
                                    PulsarAdmin.builder()
                                            .serviceHttpUrl(adminUrl)
                                            .build()
                        );
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    });
                },
                () -> {
                    applicationContext.registerBean(PulsarRecordsStorage.class, () -> {
                        return new PulsarRecordsStorage(createClient(pulsarProperties));
                    });
                }
        );
    }

    @SneakyThrows
    PulsarClient createClient(PulsarProperties pulsarProperties) {
        var clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsarProperties.getServiceUrl());

        pulsarProperties.getTlsTrustCertsFilePath().ifPresent(clientBuilder::tlsTrustCertsFilePath);
        pulsarProperties.getAuthPluginClassName().ifPresent(authClass -> {
            try {
                clientBuilder.authentication(authClass, pulsarProperties.getAuthPluginParams());
            } catch (PulsarClientException.UnsupportedAuthenticationException e) {
                throw new IllegalStateException(e);
            }
        });

        return clientBuilder.build();
    }

    @Data
    @Validated
    static class PulsarProperties {

        @NotEmpty
        String serviceUrl;

        Optional<String> adminUrl = Optional.empty();

        Optional<String> tlsTrustCertsFilePath = Optional.empty();

        Optional<String> authPluginClassName = Optional.empty();

        Map<String, String> authPluginParams = Map.of();

    }
}
