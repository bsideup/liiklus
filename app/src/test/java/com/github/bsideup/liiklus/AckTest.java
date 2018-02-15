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
import java.util.UUID;
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

    @Before
    public void setUpAckTest() throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
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
        Integer partition = stub.subscribe(Mono.just(subscribeRequest))
                .take(1)
                .delayUntil(it -> stub.ack(Mono.just(AckRequest.newBuilder().setAssignment(it.getAssignment()).setOffset(100).build())))
                .map(it -> it.getAssignment().getPartition())
                .blockFirst(Duration.ofSeconds(30));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(subscribeRequest.getTopic(), partition));
            assertThat(offsetAndMetadata)
                    .isNotNull()
                    .hasFieldOrPropertyWithValue("offset", 100 + 1L);
        });
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

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(subscribeRequest.getTopic(), partition));
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
                                .flatMap(it -> stub.receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build())))
                )
                .take(values.size())
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(records).hasSize(values.size());

        for (PartitionInfo info : kafkaConsumer.partitionsFor(subscribeRequest.getTopic())) {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(info.topic(), info.partition()));
            assertThat(offsetAndMetadata).isNull();
        }
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
