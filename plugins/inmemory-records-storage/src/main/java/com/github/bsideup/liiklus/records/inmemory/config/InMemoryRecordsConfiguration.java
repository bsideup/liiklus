package com.github.bsideup.liiklus.records.inmemory.config;

import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.records.inmemory.InMemoryRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@AutoService(LiiklusConfiguration.class)
@Slf4j
@Configuration
@GatewayProfile
@ConditionalOnProperty(value = "storage.records.type", havingValue = "MEMORY")
public class InMemoryRecordsConfiguration implements LiiklusConfiguration {

    @Bean
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

}
