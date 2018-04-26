package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.kafka.config.KafkaRecordsStorageConfiguration.KafkaProperties;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class PositionsTest extends AbstractIntegrationTest {

    @Autowired
    KafkaProperties kafkaProperties;

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
                                .publish(Mono.just(
                                        PublishRequest.newBuilder()
                                                .setTopic(subscribeRequest.getTopic())
                                                .setKey(ByteString.copyFromUtf8(key))
                                                .setValue(ByteString.copyFromUtf8("bar"))
                                                .build()
                                ))
                        )
                        .repeat(10)
                )
                .blockLast();
    }

    @Test
    public void testExternalPositions() {
        val topicPartition = new TopicPartition(subscribeRequest.getTopic(), 0);

        val props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, subscribeRequest.getGroup());

        try (val kafkaConsumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            kafkaConsumer.commitSync(ImmutableMap.of(topicPartition, new OffsetAndMetadata(5)));
        }

        ReceiveReply reply = stub
                .subscribe(Mono.just(subscribeRequest))
                .map(SubscribeReply::getAssignment)
                .filter(it -> it.getPartition() == topicPartition.partition())
                .flatMap(assignment -> stub
                        .receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(assignment).build()))
                        .delayUntil(it -> stub.ack(Mono.just(AckRequest.newBuilder().setAssignment(assignment).setOffset(it.getRecord().getOffset()).build())))
                )
                .blockFirst(Duration.ofSeconds(10));

        assertThat(reply.getRecord().getOffset())
                .isEqualTo(5);

        reply = stub
                .subscribe(Mono.just(subscribeRequest))
                .filter(it -> it.getAssignment().getPartition() == topicPartition.partition())
                .flatMap(it -> stub.receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build())))
                .blockFirst(Duration.ofSeconds(10));

        assertThat(reply.getRecord().getOffset())
                .isEqualTo(6);
    }

    @Test
    public void testGetOffsets() throws Exception {
        val key = UUID.randomUUID().toString();
        val partition = getPartitionByKey(key);

        val publishReply = stub.publish(Mono.just(
                PublishRequest.newBuilder()
                        .setTopic(subscribeRequest.getTopic())
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8("bar"))
                        .build()
        )).block(Duration.ofSeconds(10));

        assertThat(publishReply)
                .hasFieldOrPropertyWithValue("partition", partition);

        val reportedOffset = publishReply.getOffset();

        stub
                .subscribe(Mono.just(subscribeRequest))
                .filter(it -> it.getAssignment().getPartition() == partition)
                .flatMap(it -> stub.receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build()))
                        .map(ReceiveReply::getRecord)
                        .filter(record -> key.equals(record.getKey().toStringUtf8()))
                        .delayUntil(record -> stub.ack(Mono.just(AckRequest.newBuilder()
                                .setAssignment(it.getAssignment())
                                .setOffset(record.getOffset())
                                .build()
                        )))
                )
                .blockFirst(Duration.ofSeconds(10));

        val getOffsetsReply = stub
                .getOffsets(Mono.just(GetOffsetsRequest.newBuilder()
                        .setTopic(subscribeRequest.getTopic())
                        .setGroup(subscribeRequest.getGroup())
                        .build())
                )
                .block(Duration.ofSeconds(10));

        assertThat(getOffsetsReply.getOffsetsMap())
                .containsEntry(partition, reportedOffset);
    }

    @Test
    public void testGetEmptyOffsets() throws Exception {
        val getOffsetsReply = stub
                .getOffsets(Mono.just(GetOffsetsRequest.newBuilder()
                        .setTopic(subscribeRequest.getTopic())
                        .setGroup(UUID.randomUUID().toString())
                        .build())
                )
                .block(Duration.ofSeconds(10));

        assertThat(getOffsetsReply.getOffsetsMap())
                .isEmpty();

    }
}
