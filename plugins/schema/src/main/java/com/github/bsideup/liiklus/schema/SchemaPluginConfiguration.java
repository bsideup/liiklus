package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonPointer;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.google.auto.service.AutoService;
import lombok.Data;
import lombok.val;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;

import java.net.URL;

@AutoService(ApplicationContextInitializer.class)
public class SchemaPluginConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        if (!applicationContext.getEnvironment().acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        val binder = Binder.get(applicationContext.getEnvironment());
        val schemaProperties = binder.bind("schema", SchemaProperties.class).orElseGet(SchemaProperties::new);

        if (schemaProperties.isEnabled()) {
            applicationContext.registerBean(RecordPreProcessor.class, () -> {
                return new JsonSchemaPreProcessor(
                        schemaProperties.getSchemaURL(),
                        JsonPointer.compile(schemaProperties.getEventTypeJsonPointer()),
                        schemaProperties.isAllowDeprecatedProperties()
                );
            });
        }
    }

    @Data
    @Validated
    public static class SchemaProperties {

        boolean enabled;

        SchemaType type = SchemaType.JSON;

        URL schemaURL;

        String eventTypeJsonPointer = "/eventType";

        boolean allowDeprecatedProperties = false;

        enum SchemaType {
            JSON,
            ;
        }
    }

}
