package com.github.bsideup.liiklus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventBuilder;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.SignalType;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class CloudEventsTest extends AbstractIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Before
    public void setUpCloudEventsTest() throws Exception {
        processorPluginMock.getPreProcessors().add(envelope -> {
            var rawValue = envelope.getRawValue();

            if (rawValue instanceof ByteBuffer) {
                var byteBuffer = (ByteBuffer) rawValue;

                CloudEvent<?, byte[]> cloudEvent;
                try {
                    var map = MAPPER.readValue(new ByteBufferBackedInputStream(byteBuffer.duplicate()), Map.class);

                    var eventType = (String) map.remove("eventType");
                    cloudEvent = CloudEventBuilder.<byte[]>builder()
                            .withId((String) map.remove("eventId"))
                            .withType("com.example.legacy." + eventType.replace("/", ".").toLowerCase())
                            .withSource(URI.create("/tests/upcaster"))
                            .withDataContentType("application/json")
                            .withData(MAPPER.writeValueAsBytes(map))
                            .build();
                } catch (IOException e) {
                    return CompletableFuture.failedFuture(e);
                }

                return CompletableFuture.completedFuture(
                        envelope.withValue(
                                cloudEvent,
                                it -> ByteBuffer.wrap(Json.binaryEncode(it)).asReadOnlyBuffer()
                        )
                );
            }
            return CompletableFuture.completedFuture(envelope);
        });
    }

    @Test
    public void shouldSupportLegacyFormat() throws Exception {
        var subscribeAction = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        var key = "foo";
        var eventId = UUID.randomUUID().toString();
        var value = MAPPER.writeValueAsString(Map.of(
                "eventId", eventId,
                "eventType", "some/event",
                "foo", "bar"
        ));

        @SuppressWarnings("deprecation")
        var publishRequest = PublishRequest.newBuilder()
                .setTopic(subscribeAction.getTopic())
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build();
        stub.publish(publishRequest).block(Duration.ofSeconds(10));

        var record = stub.subscribe(subscribeAction)
                .flatMap(it -> stub.receive(
                        ReceiveRequest.newBuilder()
                                .setAssignment(it.getAssignment())
                                .setFormat(ReceiveRequest.ContentFormat.LIIKLUS_EVENT)
                                .build()
                ))
                .log("consumer", Level.WARNING, SignalType.ON_ERROR)
                .blockFirst(Duration.ofSeconds(60));

        assertThat(record)
                .isNotNull()
                .satisfies(it -> {
                    var event = it.getLiiklusEventRecord().getEvent();
                    assertThat(event.getId()).as("id").isEqualTo(eventId);
                    assertThat(event.getType()).as("type").isEqualTo("com.example.legacy.some.event");
                    assertThat(event.getSource()).as("source").isEqualTo("/tests/upcaster");
                    assertThat(event.getData().toStringUtf8()).as("value").isEqualTo("{\"foo\":\"bar\"}");
                });
    }
}
