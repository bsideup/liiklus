package com.github.bsideup.liiklus.records.inmemory.config;

import com.github.bsideup.liiklus.records.inmemory.InMemoryRecordsStorage;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;

@AutoService(ApplicationContextInitializer.class)
@Slf4j
public class InMemoryRecordsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();
        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        String type = environment.getRequiredProperty("storage.records.type");
        if (!"MEMORY".equals(type)) {
            return;
        }

        log.warn("\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                "=== In-memory records storage is used. Please, DO NOT run it in production. ===\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=")
        );
        applicationContext.registerBean(InMemoryRecordsStorage.class, () -> new InMemoryRecordsStorage(32));
    }
}
