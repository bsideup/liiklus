package com.github.bsideup.liiklus.pulsar.config;

import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.pulsar.PulsarRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;

@AutoService(LiiklusConfiguration.class)
@Configuration
@GatewayProfile
@EnableConfigurationProperties(PulsarRecordsStorageConfiguration.PulsarProperties.class)
@ConditionalOnProperty(value = "storage.records.type", havingValue = "PULSAR")
public class PulsarRecordsStorageConfiguration implements LiiklusConfiguration {

    @Autowired
    PulsarProperties pulsarProperties;

    @Bean
    RecordsStorage reactorPulsarSource() throws Exception {
        return new PulsarRecordsStorage(
                PulsarClient.builder().serviceUrl(pulsarProperties.getServiceUrl()).build()
        );
    }

    @Data
    @ConfigurationProperties("pulsar")
    @Validated
    public static class PulsarProperties {

        @NotEmpty
        String serviceUrl;
    }
}
