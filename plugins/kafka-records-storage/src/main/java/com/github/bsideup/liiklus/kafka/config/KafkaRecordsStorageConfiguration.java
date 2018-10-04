package com.github.bsideup.liiklus.kafka.config;

import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
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
@EnableConfigurationProperties(KafkaRecordsStorageConfiguration.KafkaProperties.class)
@ConditionalOnProperty(value = "storage.records.type", havingValue = "KAFKA")
public class KafkaRecordsStorageConfiguration implements LiiklusConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    RecordsStorage reactorKafkaSource() {
        return new KafkaRecordsStorage(
                kafkaProperties.getBootstrapServers()
        );
    }

    @Data
    @ConfigurationProperties("kafka")
    @Validated
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;
    }
}
