package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.GetEndOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class EndOffsetsTest extends AbstractIntegrationTest {

    private String topic;

    @Before
    public final void setUpEndOffsetsTest() {
        topic = testName.getMethodName();
    }

    @Test
    public void testEndOffsets() {
        var value = ByteString.copyFromUtf8("foo");

        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            for (int i = 0; i < partition + 1; i++) {
                stub.publish(PublishRequest.newBuilder()
                        .setTopic(topic)
                        .setKey(ByteString.copyFromUtf8(PARTITION_KEYS.get(partition)))
                        .setValue(value)
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
    public void testEndOffsets_unknownTopic() {
        var randomTopic = UUID.randomUUID().toString();
        var reply = stub.getEndOffsets(GetEndOffsetsRequest.newBuilder().setTopic(randomTopic).build()).block();

        assertThat(reply.getOffsetsMap()).isEmpty();
    }

}
