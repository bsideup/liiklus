package com.github.bsideup.liiklus.pulsar.config;

import com.github.bsideup.liiklus.pulsar.PulsarRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import java.util.function.Supplier;

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

        applicationContext.registerBean(RecordsStorage.class, new Supplier<RecordsStorage>() {
            @Override
            @SneakyThrows(PulsarClientException.class)
            public RecordsStorage get() {
                return new PulsarRecordsStorage(
                        PulsarClient.builder().serviceUrl(pulsarProperties.getServiceUrl()).build()
                );
            }
        });
    }

    @Data
    @Validated
    private static class PulsarProperties {

        @NotEmpty
        String serviceUrl;
    }
}
