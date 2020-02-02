package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventBuilder;
import org.assertj.core.api.Condition;
import org.junit.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class SmokeTest extends AbstractIntegrationTest {

    @Test
    public void testHealth() throws Exception {
        WebTestClient.bindToApplicationContext(applicationContext)
                .build()
                .get()
                .uri("/health")
                .exchange()
                .expectStatus()
                .is2xxSuccessful();
    }

    @Test
    public void testPrometheusExporter() throws Exception {
        WebTestClient.bindToApplicationContext(applicationContext)
                .build()
                .get()
                .uri("/prometheus")
                .exchange()
                .expectStatus()
                .is2xxSuccessful();
    }

    @Test
    public void testPublishSubscribe() throws Exception {
        SubscribeRequest subscribeAction = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        String key = "foo";
        List<String> values = IntStream.range(0, 10).mapToObj(i -> "bar-" + i).collect(Collectors.toList());
        List<ReceiveReply> records = Flux.fromIterable(values)
                .concatMap(it -> stub.publish(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeAction.getTopic())
                                .setKey(ByteString.copyFromUtf8(key))
                                .setLiiklusEvent(
                                        LiiklusEvent.newBuilder(LIIKLUS_EVENT_EXAMPLE)
                                                .setData(ByteString.copyFromUtf8(it))
                                )
                                .build()
                ))
                .thenMany(
                        stub.subscribe(subscribeAction)
                                .flatMap(it -> stub.receive(
                                        ReceiveRequest.newBuilder()
                                                .setAssignment(it.getAssignment())
                                                .setFormat(ReceiveRequest.ContentFormat.LIIKLUS_EVENT)
                                                .build()
                                ))
                )
                .take(values.size())
                .collectList()
                .log("consumer", Level.WARNING, SignalType.ON_ERROR)
                .block(Duration.ofSeconds(60));

        assertThat(records)
                .hasSize(10)
                .extracting(ReceiveReply::getLiiklusEventRecord)
                .are(new Condition<>("key is '" + key + "'") {
                    @Override
                    public boolean matches(ReceiveReply.LiiklusEventRecord value) {
                        return key.equals(value.getKey().toStringUtf8());
                    }
                })
                .extracting(it -> it.getEvent().getData().toStringUtf8())
                .containsSubsequence(values.toArray(new String[values.size()]));
    }

    @Test
    public void testNullKey() throws Exception {
        var subscribeAction = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        var value = UUID.randomUUID().toString();
        var recordsStorage = applicationContext.getBean(RecordsStorage.class);
        recordsStorage.publish(new RecordsStorage.Envelope(
                subscribeAction.getTopic(),
                null, // intentionally
                ByteBuffer.class::cast,
                CloudEventBuilder.builder()
                        .withId(UUID.randomUUID().toString())
                        .withType("com.example.event")
                        .withSource(URI.create("/tests"))
                        .withDataContentType("text/plain")
                        .withData(value.getBytes())
                        .build(),
                it -> ByteBuffer.wrap(Json.binaryEncode(it)).asReadOnlyBuffer()
        )).toCompletableFuture().join();

        var record = stub
                .subscribe(subscribeAction)
                .flatMap(it -> stub.receive(
                        ReceiveRequest.newBuilder()
                                .setAssignment(it.getAssignment())
                                .setFormat(ReceiveRequest.ContentFormat.LIIKLUS_EVENT)
                                .build()
                ))
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record)
                .isNotNull()
                .satisfies(it -> {
                    var event = it.getLiiklusEventRecord().getEvent();
                    assertThat(event.getData().toStringUtf8()).as("value").isEqualTo(value);
                });
    }
}
