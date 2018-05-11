package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@GatewayProfile
public class LayersConfiguration {

    @Autowired
    ApplicationContext applicationContext;

    @Bean
    RecordPreProcessorChain recordPreProcessorChain() {
        return new RecordPreProcessorChain(
                applicationContext.getBeansOfType(RecordPreProcessor.class).values()
        );
    }

    @Bean
    RecordPostProcessorChain recordPostProcessorChain() {
        return new RecordPostProcessorChain(
                applicationContext.getBeansOfType(RecordPostProcessor.class).values()
        );
    }
}
