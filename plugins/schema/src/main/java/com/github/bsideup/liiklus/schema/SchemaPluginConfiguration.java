package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonPointer;
import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import java.net.URL;

@AutoService(LiiklusConfiguration.class)
@GatewayProfile
@Configuration
@EnableConfigurationProperties(SchemaPluginConfiguration.SchemaProperties.class)
@ConditionalOnProperty(value = "schema.enabled", havingValue = "true")
public class SchemaPluginConfiguration implements LiiklusConfiguration {

    @Autowired
    SchemaProperties schemaProperties;

    @Bean
    JsonSchemaPreProcessor jsonSchemaPreProcessor() {
        return new JsonSchemaPreProcessor(
                schemaProperties.getSchemaURL(),
                JsonPointer.compile(schemaProperties.getEventTypeJsonPointer()),
                schemaProperties.isAllowDeprecatedProperties()
        );
    }

    @Data
    @ConfigurationProperties("schema")
    @Validated
    public static class SchemaProperties {

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
