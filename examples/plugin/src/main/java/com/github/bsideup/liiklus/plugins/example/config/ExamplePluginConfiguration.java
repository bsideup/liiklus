package com.github.bsideup.liiklus.plugins.example.config;

import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.plugins.example.ExampleRecordPostProcessor;
import com.github.bsideup.liiklus.plugins.example.ExampleRecordPreProcessor;
import com.google.auto.service.AutoService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@AutoService(LiiklusConfiguration.class)
@Configuration
public class ExamplePluginConfiguration implements LiiklusConfiguration {

    @Bean
    ExampleRecordPreProcessor encryptionRecordPreProcessor() {
        return new ExampleRecordPreProcessor();
    }

    @Bean
    ExampleRecordPostProcessor exampleRecordPostProcessor() {
        return new ExampleRecordPostProcessor();
    }
}
