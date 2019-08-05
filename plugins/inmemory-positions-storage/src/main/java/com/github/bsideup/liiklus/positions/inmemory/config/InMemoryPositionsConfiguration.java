package com.github.bsideup.liiklus.positions.inmemory.config;

import com.github.bsideup.liiklus.positions.inmemory.InMemoryPositionsStorage;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;

@AutoService(ApplicationContextInitializer.class)
@Slf4j
public class InMemoryPositionsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var type = applicationContext.getEnvironment().getRequiredProperty("storage.positions.type");

        if (!"MEMORY".equals(type)) {
            return;
        }

        log.warn("\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                "=== In-memory position storage is used. Please, DO NOT run it in production if you ACK your positions. ===\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=") + "\n" +
                String.format("%0106d", 0).replace("0", "=")
        );
        applicationContext.registerBean(InMemoryPositionsStorage.class);
    }
}
