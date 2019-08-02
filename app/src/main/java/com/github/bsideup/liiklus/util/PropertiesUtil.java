package com.github.bsideup.liiklus.util;

import lombok.NonNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

import javax.validation.Validation;
import java.util.UUID;

public class PropertiesUtil {

    public static <T> T bind(ConfigurableEnvironment environment, @NonNull T properties) {
        var configurationProperties = AnnotationUtils.findAnnotation(properties.getClass(), ConfigurationProperties.class);

        if (configurationProperties == null) {
            throw new IllegalArgumentException(properties.getClass() + " Must be annotated with @ConfigurationProperties");
        }

        var property = configurationProperties.prefix();

        // This hack is needed to trigger the validation even if no properties of T are explicitly set in the env
        // TODO remove once Spring Boot 2.2.x is out, replace with bindOrCreate
        environment.getPropertySources().addLast(new EnumerablePropertySource<Object>(properties.getClass().getName() + "_default", properties) {

            final String fakeProperty = property + "." + UUID.randomUUID().toString().replace("-", "");

            @Override
            public String[] getPropertyNames() {
                return new String[] { fakeProperty };
            }

            @Override
            public Object getProperty(String name) {
                return null;
            }
        });
        var binder = Binder.get(environment);

        var validationBindHandler = new ValidationBindHandler(
                new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator())
        );

        var bindable = Bindable.ofInstance(properties);
        return binder.bind(property, bindable, validationBindHandler).orElseGet(bindable.getValue());
    }
}
