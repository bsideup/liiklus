package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import lombok.val;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class GroupVersionTest extends AbstractIntegrationTest {

    private String topic;

    @Before
    public void setUpGroupVersionTest() throws Exception {
        topic = testName.getMethodName();

        // Will create a topic and initialize every partition
        Flux.fromIterable(PARTITION_UNIQUE_KEYS)
                .flatMap(key -> Mono
                        .defer(() -> stub
                                .publish(
                                        PublishRequest.newBuilder()
                                                .setTopic(topic)
                                                .setKey(ByteString.copyFromUtf8(key))
                                                .setValue(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                                .build()
                                )
                        )
                        .repeat(10)
                )
                .blockLast();
    }

    @Test
    public void testCompoundGroupId() throws Exception {
        val groupVersion = 1;

        val committedOffset = 3;
        val nextOffset = committedOffset + 1;

        ackOffset(groupVersion, committedOffset);

        assertThat(getRecords(Optional.of(groupVersion)).blockFirst(Duration.ofSeconds(10)).getOffset())
                .isEqualTo(nextOffset);

        // Old style should also work
        assertThat(getRecords(Optional.empty()).blockFirst(Duration.ofSeconds(10)).getOffset())
                .isEqualTo(nextOffset);
    }

    @Test
    public void testReplay() throws Exception {
        assertThat(getRecords(Optional.of(4)).take(3).collectList().block(Duration.ofSeconds(10)))
                .noneMatch(Record::getReplay);

        ackOffset(1, 3);
        ackOffset(2, 7);
        ackOffset(3, 5);

        assertThat(getRecords(Optional.of(4)).take(10).collectList().block(Duration.ofSeconds(10)))
                .extracting(Record::getOffset, Record::getReplay)
                .containsExactly(
                        tuple(0L, true),
                        tuple(1L, true),
                        tuple(2L, true),
                        tuple(3L, true),
                        tuple(4L, true),
                        tuple(5L, true),
                        tuple(6L, true),
                        tuple(7L, true), // 7 because seen by version 2
                        tuple(8L, false),
                        tuple(9L, false)
                );
    }

    private void ackOffset(int groupVersion, long offset) {
        stub
                .subscribe(
                        SubscribeRequest.newBuilder()
                                .setTopic(topic)
                                .setGroup(testName.getMethodName())
                                .setGroupVersion(groupVersion)
                                .build()
                )
                .map(SubscribeReply::getAssignment)
                .filter(it -> it.getPartition() == 0)
                .delayUntil(assignment -> stub.ack(AckRequest.newBuilder().setAssignment(assignment).setOffset(offset).build()))
                .blockFirst(Duration.ofSeconds(10));
    }

    private Flux<Record> getRecords(Optional<Integer> groupVersion) {
        return stub
                .subscribe(
                        SubscribeRequest.newBuilder()
                                .setTopic(topic)
                                .setGroup(testName.getMethodName())
                                .setGroupVersion(groupVersion.orElse(0))
                                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                                .build()
                )
                .map(SubscribeReply::getAssignment)
                .filter(it -> it.getPartition() == 0)
                .flatMap(assignment -> stub.receive(ReceiveRequest.newBuilder().setAssignment(assignment).build()))
                .map(ReceiveReply::getRecord);
    }
}
