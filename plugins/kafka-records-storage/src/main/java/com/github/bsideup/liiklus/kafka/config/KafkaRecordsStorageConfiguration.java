package com.github.bsideup.liiklus.kafka.config;

import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

import javax.validation.Validation;
import javax.validation.constraints.NotEmpty;

@AutoService(ApplicationContextInitializer.class)
public class KafkaRecordsStorageConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {


    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        if (!"KAFKA".equals(environment.getProperty("storage.records.type"))) {
            return;
        }

        var binder = Binder.get(environment);
        var validationBindHandler = new ValidationBindHandler(
                new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator())
        );

        var kafkaProperties = binder.bind("kafka", Bindable.of(KafkaProperties.class), validationBindHandler).get();

        applicationContext.registerBean(RecordsStorage.class, () -> {
            return new KafkaRecordsStorage(kafkaProperties.getBootstrapServers());
        });
    }

    @Data
    @Validated
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;
    }
}
