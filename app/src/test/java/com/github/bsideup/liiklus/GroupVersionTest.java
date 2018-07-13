package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import lombok.val;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class GroupVersionTest extends AbstractIntegrationTest {

    private static final int PARTITION = 0;

    public static final int NUM_OF_RECORDS_PER_PARTITION = 10;

    private String topic;

    @Before
    public void setUpGroupVersionTest() throws Exception {
        topic = testName.getMethodName();

        Flux.range(0, NUM_OF_RECORDS_PER_PARTITION)
                .flatMap(__ -> stub.publish(
                        PublishRequest.newBuilder()
                                .setTopic(topic)
                                .setKey(ByteString.copyFromUtf8(PARTITION_KEYS.get(PARTITION)))
                                .setValue(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                .build()
                ))
                .blockLast(Duration.ofSeconds(10));
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
    public void testReplayWithNoAck() throws Exception {
        assertThat(getAllRecords(0)).noneMatch(Record::getReplay);
        assertThat(getAllRecords(1)).noneMatch(Record::getReplay);
    }

    @Test
    public void testReplayUsesAlwaysLatest() throws Exception {
        ackOffset(1, 3);
        ackOffset(2, 7);
        ackOffset(3, 5);

        assertThat(getAllRecords(4))
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

    @Test
    public void testReplayWithPreviousVersion() throws Exception {
        ackOffset(2, 7);

        assertThat(getAllRecords(1))
                .extracting(Record::getOffset, Record::getReplay)
                .containsExactly(
                        tuple(0L, true),
                        tuple(1L, true),
                        tuple(2L, true),
                        tuple(3L, true),
                        tuple(4L, true),
                        tuple(5L, true),
                        tuple(6L, true),
                        tuple(7L, true),
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

    private List<Record> getAllRecords(Integer groupVersion) {
        return getRecords(Optional.of(groupVersion)).take(NUM_OF_RECORDS_PER_PARTITION)
                .collectList()
                .block(Duration.ofSeconds(10));
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
