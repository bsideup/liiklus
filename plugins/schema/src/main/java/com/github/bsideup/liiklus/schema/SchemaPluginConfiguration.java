package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonPointer;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.hibernate.validator.group.GroupSequenceProvider;
import org.hibernate.validator.spi.group.DefaultGroupSequenceProvider;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

import javax.validation.Validation;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@AutoService(ApplicationContextInitializer.class)
public class SchemaPluginConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        if (!applicationContext.getEnvironment().acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var binder = Binder.get(applicationContext.getEnvironment());
        var validationBindHandler = new ValidationBindHandler(
                new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator())
        );
        var schemaProperties = binder.bind("schema", Bindable.of(SchemaProperties.class), validationBindHandler)
                .orElseGet(SchemaProperties::new);

        if (!schemaProperties.isEnabled()) {
            return;
        }

        applicationContext.registerBean(RecordPreProcessor.class, () -> {
            return new JsonSchemaPreProcessor(
                    schemaProperties.getSchemaURL(),
                    JsonPointer.compile(schemaProperties.getEventTypeJsonPointer()),
                    schemaProperties.isAllowDeprecatedProperties()
            );
        });
    }

    @Data
    @Validated
    @GroupSequenceProvider(SchemaProperties.EnabledSequenceProvider.class)
    static class SchemaProperties {

        boolean enabled = false;

        @NotNull(groups = Enabled.class)
        SchemaType type = SchemaType.JSON;

        @NotNull(groups = Enabled.class)
        URL schemaURL;

        @NotEmpty(groups = Enabled.class)
        String eventTypeJsonPointer = "/eventType";

        boolean allowDeprecatedProperties = false;

        enum SchemaType {
            JSON,
            ;
        }

        interface Enabled {}

        public static class EnabledSequenceProvider implements DefaultGroupSequenceProvider<SchemaProperties> {

            @Override
            public List<Class<?>> getValidationGroups(SchemaProperties object) {
                var sequence = new ArrayList<Class<?>>();
                sequence.add(SchemaProperties.class);
                if (object != null && object.isEnabled()) {
                    sequence.add(Enabled.class);
                }
                return sequence;
            }
        }
    }

}
