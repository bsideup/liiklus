package com.github.bsideup.liiklus.kafka.config;

import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import java.util.HashMap;
import java.util.Map;

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

        var kafkaProperties = binder.bind("kafka", KafkaProperties.class).get();

        applicationContext.registerBean(RecordsStorage.class, () -> new KafkaRecordsStorage(
                kafkaProperties.getBootstrapServers(),
                kafkaProperties.getBaseProps()
        ));
    }

    @Data
    @Validated
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;

        Map<String, String> baseProps = new HashMap<>();

    }
}
