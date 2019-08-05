package com.github.bsideup.liiklus.util;

import lombok.NonNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

import javax.validation.Validation;

public class PropertiesUtil {

    public static <T> T bind(ConfigurableEnvironment environment, @NonNull T properties) {
        var configurationProperties = AnnotationUtils.findAnnotation(properties.getClass(), ConfigurationProperties.class);

        if (configurationProperties == null) {
            throw new IllegalArgumentException(properties.getClass() + " Must be annotated with @ConfigurationProperties");
        }

        var property = configurationProperties.prefix();

        var validationBindHandler = new ValidationBindHandler(
                new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator())
        );

        var bindable = Bindable.ofInstance(properties);
        return Binder.get(environment).bind(property, bindable, validationBindHandler).orElseGet(bindable.getValue());
    }
}
