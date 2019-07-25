package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.service.LiiklusService;
import lombok.Data;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class GatewayConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var binder = Binder.get(environment);
        var layersProperties = binder.bind("layers", LayersProperties.class).orElseGet(LayersProperties::new);

        var comparator = Comparator
                .comparingInt(it -> layersProperties.getOrders().getOrDefault(it.getClass().getName(), 0))
                .thenComparing(it -> it.getClass().getName());

        applicationContext.registerBean(RecordPreProcessorChain.class, () -> new RecordPreProcessorChain(
                applicationContext.getBeansOfType(RecordPreProcessor.class).values().stream()
                        .sorted(comparator)
                        .collect(Collectors.toList())
        ));

        applicationContext.registerBean(RecordPostProcessorChain.class, () -> new RecordPostProcessorChain(
                applicationContext.getBeansOfType(RecordPostProcessor.class).values().stream()
                        .sorted(comparator.reversed())
                        .collect(Collectors.toList())
        ));

        applicationContext.registerBean(LiiklusService.class);
    }

    @Data
    static class LayersProperties {

        Map<String, Integer> orders = new HashMap<>();

    }
}
