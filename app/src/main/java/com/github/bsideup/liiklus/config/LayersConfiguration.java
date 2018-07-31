package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@GatewayProfile
@EnableConfigurationProperties(LayersConfiguration.LayersProperties.class)
public class LayersConfiguration {

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    LayersProperties layersProperties;

    @Bean
    RecordPreProcessorChain recordPreProcessorChain() {
        return new RecordPreProcessorChain(
                applicationContext.getBeansOfType(RecordPreProcessor.class).values().stream()
                        .sorted(getComparator())
                        .collect(Collectors.toList())
        );
    }

    @Bean
    RecordPostProcessorChain recordPostProcessorChain() {
        return new RecordPostProcessorChain(
                applicationContext.getBeansOfType(RecordPostProcessor.class).values().stream()
                        .sorted(getComparator().reversed())
                        .collect(Collectors.toList())
        );
    }

    private <T> Comparator<T> getComparator() {
        return Comparator
                .<T>comparingInt(it -> layersProperties.getOrders().getOrDefault(it.getClass().getName(), 0))
                .thenComparing(it -> it.getClass().getName());
    }

    @Data
    @ConfigurationProperties("layers")
    static class LayersProperties {

        Map<String, Integer> orders = new HashMap<>();

    }
}
