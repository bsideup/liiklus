package com.github.bsideup.liiklus.plugins.example.config;

import com.github.bsideup.liiklus.plugins.example.ExampleRecordPostProcessor;
import com.github.bsideup.liiklus.plugins.example.ExampleRecordPreProcessor;
import com.google.auto.service.AutoService;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;

@AutoService(ApplicationContextInitializer.class)
public class ExamplePluginConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        applicationContext.registerBean(ExampleRecordPreProcessor.class);
        applicationContext.registerBean(ExampleRecordPostProcessor.class);
    }
}
