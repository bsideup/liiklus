package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventBuilder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSchemaPreProcessorTest {

    @Test
    void testBasicValidation() {
        var processor = getProcessor();

        assertThatThrownBy(() -> preProcess(processor, "simpleEvent"))
                .hasMessageContaining("$.requiredField: is missing but it is required");

        preProcess(processor, "simpleEvent", it -> it.put("requiredField", "all good"));

        assertThatThrownBy(() -> preProcess(processor, "simpleEvent", it -> it.put("intField", "string")))
                .hasMessageContaining("$.requiredField: is missing but it is required")
                .hasMessageContaining("$.intField: string found, integer expected");
    }

    @Test
    void testDeprecatedField() {
        preProcess(getProcessor(true), "withDeprecatedField", it -> it.put("deprecatedField", "boo"));

        assertThatThrownBy(() -> preProcess(getProcessor(false), "withDeprecatedField", it -> it.put("deprecatedField", "boo")))
                .hasMessageContaining("$.deprecatedField: is deprecated");
    }

    @Test
    void testDeprecatedEventNotAllowed() {
        preProcess(getProcessor(true), "deprecatedEvent");

        assertThatThrownBy(() -> preProcess(getProcessor(false), "deprecatedEvent"))
                .hasMessageContaining("$: is deprecated");
    }

    @Test
    void testTypeWithSlashes() {
        var processor = getProcessor();

        preProcess(processor, "event/type/with/slashes", it -> it.put("foo", "bar"));

        assertThatThrownBy(() -> preProcess(processor, "event/type/with/slashes", it -> it.put("foo", 123)))
                .hasMessageContaining("$.foo: integer found, string expected");
    }

    @Test
    void testMissingEventType() {
        var processor = getProcessor();

        assertThatThrownBy(() -> preProcess(processor, (String) null))
                .hasMessageContaining("/eventType is null");
    }

    @Test
    void testCloudEvent() throws Exception {
        var processor = getProcessor();

        assertThatThrownBy(() -> {
            preProcess(processor,
                    CloudEventBuilder.builder()
                            .withId(UUID.randomUUID().toString())
                            .withType("com.example.cloudevent")
                            .withSource(URI.create("/tests"))
                            .withDataContentType("application/json")
                            .withData(
                                    JsonSchemaPreProcessor.JSON_MAPPER.writeValueAsBytes(
                                            Map.of("foo", 123)
                                    )
                            )
                            .build()
            );
        }).hasMessageContaining("$.foo: integer found, string expected");

        preProcess(
                processor,
                CloudEventBuilder.builder()
                        .withId(UUID.randomUUID().toString())
                        .withType("com.example.cloudevent")
                        .withSource(URI.create("/tests"))
                        .withDataContentType("application/json")
                        .withData(
                                JsonSchemaPreProcessor.JSON_MAPPER.writeValueAsBytes(
                                        Map.of("foo", "bar")
                                )
                        )
                        .build()
        );
    }

    @Test
    void testCloudEventWithCompatibleMediaType() {
        var processor = getProcessor();

        assertThatThrownBy(() -> {
            preProcess(processor,
                    CloudEventBuilder.builder()
                            .withId(UUID.randomUUID().toString())
                            .withType("com.example.cloudevent")
                            .withSource(URI.create("/tests"))
                            .withDataContentType("application/json;v2")
                            .withData(
                                    JsonSchemaPreProcessor.JSON_MAPPER.writeValueAsBytes(
                                            Map.of("foo", 123)
                                    )
                            )
                            .build()
            );
        }).hasMessageContaining("$.foo: integer found, string expected");
    }

    @Test
    void testCloudEventWithWrongMimeType() {
        var processor = getProcessor();

        assertThatThrownBy(() -> {
            preProcess(processor,
                    CloudEventBuilder.builder()
                            .withId(UUID.randomUUID().toString())
                            .withType("com.example.cloudevent")
                            .withSource(URI.create("/tests"))
                            .withDataContentType("text/plain")
                            .withData(
                                    JsonSchemaPreProcessor.JSON_MAPPER.writeValueAsBytes(
                                            Map.of("foo", 123)
                                    )
                            )
                            .build()
            );
        }).hasMessageContaining("Media type isn't compatible with 'application/json'");
    }

    private JsonSchemaPreProcessor getProcessor() {
        return getProcessor(true);
    }

    private JsonSchemaPreProcessor getProcessor(boolean allowDeprecatedProperties) {
        return new JsonSchemaPreProcessor(
                getSchema("basic.yml"),
                JsonPointer.compile("/eventType"),
                allowDeprecatedProperties
        );
    }

    private URL getSchema(String name) {
        return Thread.currentThread().getContextClassLoader().getResource("schemas/" + name);
    }

    private void preProcess(JsonSchemaPreProcessor processor, String eventType) {
        preProcess(processor, eventType, __ -> {
        });
    }

    @SneakyThrows
    private void preProcess(JsonSchemaPreProcessor processor, String eventType, Consumer<ObjectNode> nodeConsumer) {
        ObjectNode node = JsonSchemaPreProcessor.JSON_MAPPER.createObjectNode();
        node.put("eventType", eventType);
        nodeConsumer.accept(node);
        try {
            processor.preProcess(new Envelope(
                    "topic",
                    null,
                    __ -> ByteBuffer.wrap("key".getBytes()),
                    node,
                    __ -> {
                        try {
                            return ByteBuffer.wrap(JsonSchemaPreProcessor.JSON_MAPPER.writeValueAsBytes(node));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
            )).toCompletableFuture().get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    private void preProcess(JsonSchemaPreProcessor processor, CloudEvent<?, ?> cloudEvent) {
        try {
            processor.preProcess(new Envelope(
                    "topic",
                    null,
                    __ -> ByteBuffer.wrap("key".getBytes()),
                    cloudEvent,
                    it -> ByteBuffer.wrap(Json.binaryEncode(it)).asReadOnlyBuffer()
            )).toCompletableFuture().get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

}