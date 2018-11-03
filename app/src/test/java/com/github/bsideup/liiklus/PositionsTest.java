package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class PositionsTest extends AbstractIntegrationTest {

    SubscribeRequest subscribeRequest;

    @Before
    public void setUpConsumerGroupsTest() throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        // Will create a topic and initialize every partition
        Flux.fromIterable(PARTITION_UNIQUE_KEYS)
                .flatMap(key -> Mono
                        .defer(() -> stub
                                .publish(
                                        PublishRequest.newBuilder()
                                                .setTopic(subscribeRequest.getTopic())
                                                .setKey(ByteString.copyFromUtf8(key))
                                                .setValue(ByteString.copyFromUtf8("bar"))
                                                .build()
                                )
                        )
                        .repeat(10)
                )
                .blockLast();
    }

    @Test
    public void testGetOffsets() throws Exception {
        var key = UUID.randomUUID().toString();
        var partition = getPartitionByKey(key);

        var publishReply = stub.publish(
                PublishRequest.newBuilder()
                        .setTopic(subscribeRequest.getTopic())
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8("bar"))
                        .build()
        ).block(Duration.ofSeconds(10));

        assertThat(publishReply)
                .hasFieldOrPropertyWithValue("partition", partition);

        var reportedOffset = publishReply.getOffset();

        stub
                .subscribe(subscribeRequest)
                .filter(it -> it.getAssignment().getPartition() == partition)
                .flatMap(it -> stub.receive(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build())
                        .map(ReceiveReply::getRecord)
                        .filter(record -> key.equals(record.getKey().toStringUtf8()))
                        .delayUntil(record -> stub.ack(
                                AckRequest.newBuilder()
                                        .setAssignment(it.getAssignment())
                                        .setOffset(record.getOffset())
                                        .build()
                        ))
                )
                .blockFirst(Duration.ofSeconds(10));

        var getOffsetsReply = stub
                .getOffsets(
                        GetOffsetsRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setGroup(subscribeRequest.getGroup())
                                .build()
                )
                .block(Duration.ofSeconds(10));

        assertThat(getOffsetsReply.getOffsetsMap())
                .containsEntry(partition, reportedOffset);
    }

    @Test
    public void testGetEmptyOffsets() throws Exception {
        var getOffsetsReply = stub
                .getOffsets(
                        GetOffsetsRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setGroup(UUID.randomUUID().toString())
                                .build()
                )
                .block(Duration.ofSeconds(10));

        assertThat(getOffsetsReply.getOffsetsMap())
                .isEmpty();

    }
}
