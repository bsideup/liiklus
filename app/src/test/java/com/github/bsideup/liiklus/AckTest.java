package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class AckTest extends AbstractIntegrationTest {

    SubscribeRequest subscribeRequest;

    @BeforeEach
    void setUpAckTest(TestInfo info) throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(info.getTestMethod().map(Method::getName).orElse("unknown"))
                .setGroup(info.getTestMethod().map(Method::getName).orElse("unknown"))
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        // Will create a topic
        stub
                .publish(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setLiiklusEvent(LIIKLUS_EVENT_EXAMPLE)
                                .build()
                )
                .block();
    }

    @Test
    void testManualAck() throws Exception {
        Integer partition = stub.subscribe(subscribeRequest)
                .take(1)
                .delayUntil(it -> {
                    return stub.ack(
                            AckRequest.newBuilder()
                                    .setTopic(subscribeRequest.getTopic())
                                    .setGroup(subscribeRequest.getGroup())
                                    .setGroupVersion(subscribeRequest.getGroupVersion())
                                    .setPartition(it.getAssignment().getPartition())
                                    .setOffset(100)
                                    .build()
                    );
                })
                .map(it -> it.getAssignment().getPartition())
                .blockFirst(Duration.ofSeconds(30));

        Map<Integer, Long> positions = stub
                .getOffsets(
                        GetOffsetsRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setGroup(subscribeRequest.getGroup())
                                .build()
                )
                .map(GetOffsetsReply::getOffsetsMap)
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .isNotNull()
                .containsEntry(partition, 100L);
    }

    @Test
    void testStatelessAck() throws Exception {
        int partition = 1;
        int groupVersion = 1;
        AckRequest ackRequest = AckRequest.newBuilder()
                .setTopic(subscribeRequest.getTopic())
                .setGroup(subscribeRequest.getGroup())
                .setGroupVersion(groupVersion)
                .setPartition(partition)
                .setOffset(100)
                .build();

        stub.ack(ackRequest).block(Duration.ofSeconds(10));

        Map<Integer, Long> positions = stub
                .getOffsets(
                        GetOffsetsRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setGroup(subscribeRequest.getGroup())
                                .setGroupVersion(groupVersion)
                                .build()
                )
                .map(GetOffsetsReply::getOffsetsMap)
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .isNotNull()
                .containsEntry(partition, 100L);
    }

    @Test
    void testAlwaysLatest() throws Exception {
        Integer partition = stub.subscribe(subscribeRequest)
                .map(SubscribeReply::getAssignment)
                .delayUntil(new Function<>() {
                    @Override
                    @SuppressWarnings("deprecation")
                    public Publisher<?> apply(Assignment assignment) {
                        return stub.ack(AckRequest.newBuilder().setAssignment(assignment).setOffset(10).build())
                                .then(stub.ack(AckRequest.newBuilder().setAssignment(assignment).setOffset(200).build()))
                                .then(stub.ack(AckRequest.newBuilder().setAssignment(assignment).setOffset(100).build()));
                    }
                })
                .take(1)
                .map(Assignment::getPartition)
                .blockFirst(Duration.ofSeconds(10));

        Map<Integer, Long> positions = stub
                .getOffsets(
                        GetOffsetsRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setGroup(subscribeRequest.getGroup())
                                .build()
                )
                .map(GetOffsetsReply::getOffsetsMap)
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .isNotNull()
                .containsEntry(partition, 100L);
    }

    @Test
    void testInterruption() throws Exception {
        String key = "some key";
        int partition = getPartitionByKey(key);
        ByteString keyBytes = ByteString.copyFromUtf8(key);

        Map<String, Integer> receiveStatus = Flux.range(0, 10)
                .concatMap(i -> stub.publish(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setKey(keyBytes)
                                .setLiiklusEvent(
                                        LiiklusEvent.newBuilder(LIIKLUS_EVENT_EXAMPLE)
                                                .setData(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                )
                                .build()
                ))
                .thenMany(
                        Flux
                                .defer(() -> stub
                                        .subscribe(subscribeRequest)
                                        .filter(it -> it.getAssignment().getPartition() == partition)
                                        .flatMap(it -> stub
                                                .receive(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build())
                                                .map(ReceiveReply::getRecord)
                                                .buffer(5)
                                                .delayUntil(batch -> {
                                                    @SuppressWarnings("deprecation")
                                                    var builder = AckRequest.newBuilder()
                                                            .setAssignment(it.getAssignment());

                                                    return stub.ack(
                                                            builder
                                                                    .setOffset(batch.get(batch.size() - 1).getOffset())
                                                                    .build()
                                                    );
                                                })
                                        )
                                        .take(1)
                                )
                                .repeat()
                                .flatMapIterable(batch -> batch)
                )
                .take(10)
                .map(it -> it.getValue().toStringUtf8())
                .scan(new HashMap<String, Integer>(), (acc, value) -> {
                    acc.compute(value, (__, currentCount) -> currentCount == null ? 1 : currentCount + 1);
                    return acc;
                })
                .blockLast(Duration.ofSeconds(30));

        assertThat(receiveStatus)
                .hasSize(10)
                .allSatisfy((__, value) -> {
                    assertThat(value).isEqualTo(1);
                });
    }
}
