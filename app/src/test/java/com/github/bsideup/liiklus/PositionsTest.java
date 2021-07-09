package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PositionsTest extends AbstractIntegrationTest {

    SubscribeRequest subscribeRequest;

    @BeforeEach
    void setUpConsumerGroupsTest(TestInfo info) throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(info.getTestMethod().map(Method::getName).orElse("unknown"))
                .setGroup(info.getTestMethod().map(Method::getName).orElse("unknown"))
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
                                                .setLiiklusEvent(LIIKLUS_EVENT_EXAMPLE)
                                                .build()
                                )
                        )
                        .repeat(10)
                )
                .blockLast();
    }

    @Test
    void testGetOffsets() throws Exception {
        var key = UUID.randomUUID().toString();
        var partition = getPartitionByKey(key);

        var publishReply = stub.publish(
                PublishRequest.newBuilder()
                        .setTopic(subscribeRequest.getTopic())
                        .setKey(ByteString.copyFromUtf8(key))
                        .setLiiklusEvent(LIIKLUS_EVENT_EXAMPLE)
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
                        .delayUntil(record -> {
                            @SuppressWarnings("deprecation")
                            var builder = AckRequest.newBuilder()
                                    .setAssignment(it.getAssignment());
                            return stub.ack(
                                    builder
                                            .setOffset(record.getOffset())
                                            .build()
                            );
                        })
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
    void testGetEmptyOffsets() throws Exception {
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
