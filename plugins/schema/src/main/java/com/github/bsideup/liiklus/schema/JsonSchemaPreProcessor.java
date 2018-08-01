package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;
import com.github.bsideup.liiklus.schema.internal.DeprecatedKeyword;
import com.networknt.schema.JsonMetaSchema;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@FieldDefaults(makeFinal = true)
@Slf4j
public class JsonSchemaPreProcessor implements RecordPreProcessor {

    static ObjectMapper JSON_MAPPER = new ObjectMapper();

    JsonPointer eventTypePointer;

    JsonSchemaFactory jsonSchemaFactory;

    JsonSchema schema;

    final ConcurrentMap<String, Optional<JsonSchema>> schemas = new ConcurrentHashMap<>();

    public JsonSchemaPreProcessor(URL schemaURL, JsonPointer eventTypePointer, boolean allowDeprecatedProperties) {
        this.eventTypePointer = eventTypePointer;

        val jsonSchemaFactoryBuilder = JsonSchemaFactory.builder(JsonSchemaFactory.getInstance());

        val jsonMetaSchemaBuilder = JsonMetaSchema.builder(JsonMetaSchema.getDraftV4().getUri(), JsonMetaSchema.getDraftV4());
        if (!allowDeprecatedProperties) {
            jsonMetaSchemaBuilder.addKeyword(new DeprecatedKeyword());
        }

        jsonSchemaFactoryBuilder.addMetaSchema(jsonMetaSchemaBuilder.build());

        this.jsonSchemaFactory = jsonSchemaFactoryBuilder
                .urlFetcher(url -> new ByteArrayInputStream(JSON_MAPPER.writeValueAsBytes(new YAMLMapper().readTree(url))))
                .build();

        this.schema = jsonSchemaFactory.getSchema(schemaURL);
    }

    @Override
    public CompletionStage<Envelope> preProcess(Envelope envelope) {
        try {
            val event = envelope.getRawValue() instanceof ObjectNode
                    ? (ObjectNode) envelope.getRawValue()
                    : (ObjectNode) JSON_MAPPER.readTree(new ByteBufferBackedInputStream(envelope.getValue().duplicate()));

            val eventType = event.at(eventTypePointer).asText();
            JsonSchema eventSchema = schemas
                    .computeIfAbsent(eventType, key -> {
                        try {
                            val refSchemaNode = (ObjectNode) schema.getRefSchemaNode("#/events/" + URLEncoder.encode(key, "utf-8"));

                            val refSchema = jsonSchemaFactory.getSchema(refSchemaNode);
                            return Optional.of(refSchema);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return Optional.empty();
                        }
                    })
                    .orElseThrow(() -> new IllegalStateException("No schema for '" + eventType + "'"));

            val validationMessages = eventSchema.validate(event);

            if (validationMessages.isEmpty()) {
                return CompletableFuture.completedFuture(envelope);
            } else {
                val result = new CompletableFuture<Envelope>();
                val message = validationMessages.stream().map(ValidationMessage::toString).collect(Collectors.joining("\n"));
                result.completeExceptionally(new IllegalStateException(message));
                return result;
            }
        } catch (Exception e) {
            val result = new CompletableFuture<Envelope>();
            result.completeExceptionally(e);
            return result;
        }
    }
}
