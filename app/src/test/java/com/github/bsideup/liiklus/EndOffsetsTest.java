package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.GetEndOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Method;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EndOffsetsTest extends AbstractIntegrationTest {

    private String topic;

    @BeforeEach
    final void setUpEndOffsetsTest(TestInfo info) {
        topic = info.getTestMethod().map(Method::getName).orElse("unknown");
    }

    @Test
    void testEndOffsets() {
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            for (int i = 0; i < partition + 1; i++) {
                stub.publish(PublishRequest.newBuilder()
                        .setTopic(topic)
                        .setKey(ByteString.copyFromUtf8(PARTITION_KEYS.get(partition)))
                        .setLiiklusEvent(LIIKLUS_EVENT_EXAMPLE)
                        .build()
                ).block();
            }
        }

        var reply = stub.getEndOffsets(GetEndOffsetsRequest.newBuilder().setTopic(topic).build()).block();

        assertThat(reply.getOffsetsMap())
                .hasSize(NUM_PARTITIONS)
                .allSatisfy((partition, offset) -> {
                    assertThat(offset)
                            .as("offset of p" + partition)
                            .isEqualTo(partition.longValue());
                });
    }

    @Test
    void testEndOffsets_unknownTopic() {
        var randomTopic = UUID.randomUUID().toString();
        var reply = stub.getEndOffsets(GetEndOffsetsRequest.newBuilder().setTopic(randomTopic).build()).block();

        assertThat(reply.getOffsetsMap()).isEmpty();
    }

}
