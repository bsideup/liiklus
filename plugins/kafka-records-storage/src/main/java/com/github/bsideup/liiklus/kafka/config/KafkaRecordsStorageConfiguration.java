package com.github.bsideup.liiklus.kafka.config;

import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;

@AutoService(ApplicationContextInitializer.class)
public class KafkaRecordsStorageConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        if (!"KAFKA".equals(environment.getRequiredProperty("storage.records.type"))) {
            return;
        }

        var kafkaProperties = PropertiesUtil.bind(environment, new KafkaProperties());

        applicationContext.registerBean(KafkaRecordsStorage.class, () -> {
            return new KafkaRecordsStorage(kafkaProperties.getBootstrapServers());
        });
    }

    @ConfigurationProperties("kafka")
    @Data
    @Validated
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;
    }
}
