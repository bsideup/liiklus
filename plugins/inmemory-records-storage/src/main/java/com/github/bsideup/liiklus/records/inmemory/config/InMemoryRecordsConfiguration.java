package com.github.bsideup.liiklus.records.inmemory.config;

import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.records.inmemory.InMemoryRecordsStorage;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;

@AutoService(ApplicationContextInitializer.class)
@Slf4j
public class InMemoryRecordsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    RecordsStorage inMemoryRecordsStorage() {
        log.warn("\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                "=== In-memory records storage is used. Please, DO NOT run it in production. ===\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=")
        );
        return new InMemoryRecordsStorage(32);
    }

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        if (!applicationContext.getEnvironment().acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }
        String type = applicationContext.getEnvironment().getProperty("storage.records.type");
        if ("MEMORY".equals(type)) {
            applicationContext.registerBean(RecordsStorage.class, this::inMemoryRecordsStorage);
        }
    }
}
