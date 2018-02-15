package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.config.KafkaConfiguration.KafkaProperties;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AckTest extends AbstractIntegrationTest {

    @Autowired
    KafkaProperties kafkaProperties;

    KafkaConsumer<?, ?> kafkaConsumer;

    SubscribeRequest subscribeRequest;

    AckRequest baseAckRequest;

    @Before
    public void setUpAckTest() throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        baseAckRequest = AckRequest.newBuilder()
                .setTopic(subscribeRequest.getTopic())
                .setGroup(subscribeRequest.getGroup())
                .build();

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, subscribeRequest.getGroup());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.listTopics();

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
        AckRequest ackRequest = baseAckRequest.toBuilder().setPartition(0).setOffset(100).build();

        stub.subscribe(Mono.just(subscribeRequest))
                .filter(it -> it.getAssignment().getPartition() == ackRequest.getPartition())
                .delayUntil(__ -> stub.ack(Mono.just(ackRequest)))
                .blockFirst(Duration.ofSeconds(10));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(subscribeRequest.getTopic(), ackRequest.getPartition()));
            assertThat(offsetAndMetadata)
                    .isNotNull()
                    .hasFieldOrPropertyWithValue("offset", 100 + 1L);
        });
    }

    @Test
    public void testAlwaysLatest() throws Exception {
        AckRequest ackRequest = baseAckRequest.toBuilder().setPartition(0).setOffset(10).build();

        stub.subscribe(Mono.just(subscribeRequest))
                .filter(it -> it.getAssignment().getPartition() == ackRequest.getPartition())
                .delayUntil(__ -> stub.ack(Mono.just(ackRequest)))
                .delayUntil(__ -> stub.ack(Mono.just(ackRequest.toBuilder().setOffset(200).build())))
                .delayUntil(__ -> stub.ack(Mono.just(ackRequest.toBuilder().setOffset(100).build())))
                .blockFirst(Duration.ofSeconds(10));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(subscribeRequest.getTopic(), ackRequest.getPartition()));
            assertThat(offsetAndMetadata)
                    .isNotNull()
                    .hasFieldOrPropertyWithValue("offset", 100 + 1L);
        });
    }

    @Test
    public void testNoCommitIfNotAcked() throws Exception {
        List<String> values = IntStream.range(0, 10).mapToObj(i -> "bar-" + i).collect(Collectors.toList());
        List<ReceiveReply> records = Flux.fromIterable(values)
                .flatMapSequential(it -> stub.publish(Mono.just(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeRequest.getTopic())
                                .setValue(ByteString.copyFromUtf8(it))
                                .build()
                )))
                .thenMany(
                        stub.subscribe(Mono.just(subscribeRequest))
                                .flatMap(it -> stub.receive(Mono.just(
                                        ReceiveRequest.newBuilder()
                                                .setGroup(subscribeRequest.getGroup())
                                                .setTopic(subscribeRequest.getTopic())
                                                .setPartition(it.getAssignment().getPartition())
                                                .build()
                                )))
                )
                .take(values.size())
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(records).hasSize(10);

        for (PartitionInfo info : kafkaConsumer.partitionsFor(subscribeRequest.getTopic())) {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(info.topic(), info.partition()));
            assertThat(offsetAndMetadata).isNull();
        }
    }
}
