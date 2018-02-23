package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class AckTest extends AbstractIntegrationTest {

    @Autowired
    PositionsStorage positionsStorage;

    SubscribeRequest subscribeRequest;

    @Before
    public void setUpAckTest() throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        // Will create a topic
        stub
                .publish(Mono.just(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setValue(ByteString.copyFromUtf8("bar"))
                                .build()
                ))
                .block();
    }

    @Test
    public void testManualAck() throws Exception {
        Integer partition = stub.subscribe(Mono.just(subscribeRequest))
                .take(1)
                .delayUntil(it -> stub.ack(Mono.just(AckRequest.newBuilder().setAssignment(it.getAssignment()).setOffset(100).build())))
                .map(it -> it.getAssignment().getPartition())
                .blockFirst(Duration.ofSeconds(30));

        Map<Integer, Long> positions = positionsStorage.fetch(subscribeRequest.getTopic(), subscribeRequest.getGroup(), Sets.newHashSet(partition), emptyMap()).toCompletableFuture().get();
        assertThat(positions)
                .isNotNull()
                .containsEntry(partition, 100L);
    }

    @Test
    public void testAlwaysLatest() throws Exception {
        Integer partition = stub.subscribe(Mono.just(subscribeRequest))
                .map(SubscribeReply::getAssignment)
                .concatMap(assignment ->
                        stub.ack(Mono.just(AckRequest.newBuilder().setAssignment(assignment).setOffset(10).build()))
                                .then(stub.ack(Mono.just(AckRequest.newBuilder().setAssignment(assignment).setOffset(200).build())))
                                .then(stub.ack(Mono.just(AckRequest.newBuilder().setAssignment(assignment).setOffset(100).build())))
                                .then(Mono.just(assignment))
                )
                .take(1)
                .map(Assignment::getPartition)
                .blockFirst(Duration.ofSeconds(10));

        Map<Integer, Long> positions = positionsStorage.fetch(subscribeRequest.getTopic(), subscribeRequest.getGroup(), Sets.newHashSet(partition), emptyMap()).toCompletableFuture().get();
        assertThat(positions)
                .isNotNull()
                .containsEntry(partition, 100L);
    }

    @Test
    public void testInterruption() throws Exception {
        ByteString key = ByteString.copyFromUtf8(UUID.randomUUID().toString());

        Map<String, Integer> receiveStatus = Flux.fromStream(IntStream.range(0, 10).boxed())
                .concatMap(i -> stub.publish(Mono.just(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setKey(key)
                                .setValue(ByteString.copyFromUtf8("foo-" + i))
                                .build()
                )))
                .thenMany(
                        Flux
                                .defer(() -> stub
                                        .subscribe(Mono.just(subscribeRequest))
                                        .flatMap(it -> stub
                                                .receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build()))
                                                .map(ReceiveReply::getRecord)
                                                .filter(record -> key.equals(record.getKey()))
                                                .buffer(5)
                                                .delayUntil(batch -> stub
                                                        .ack(Mono.just(AckRequest.newBuilder()
                                                                .setAssignment(it.getAssignment())
                                                                .setOffset(batch.get(batch.size() - 1).getOffset())
                                                                .build()
                                                        ))
                                                )
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

        assertThat(receiveStatus.values())
                .hasSize(10)
                .containsOnly(1);
    }
}
