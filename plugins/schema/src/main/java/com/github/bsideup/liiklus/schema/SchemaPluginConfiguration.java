package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonPointer;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.hibernate.validator.group.GroupSequenceProvider;
import org.hibernate.validator.spi.group.DefaultGroupSequenceProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@AutoService(ApplicationContextInitializer.class)
public class SchemaPluginConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();
        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var schemaProperties = PropertiesUtil.bind(environment, new SchemaProperties());

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

    @ConfigurationProperties("schema")
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
