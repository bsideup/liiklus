package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.source.ReactorKafkaSource;
import lombok.Data;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaConfiguration.KafkaProperties.class)
public class KafkaConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    ReactorKafkaSource reactorKafkaSource() {
        return new ReactorKafkaSource(
                kafkaProperties.getBootstrapServers()
        );
    }

    @Data
    @ConfigurationProperties("kafka")
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;
    }
}
