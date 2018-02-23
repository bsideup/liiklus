package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.kafka.config.KafkaRecordsStorageConfiguration.KafkaProperties;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
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

        val record = stub
                .subscribe(Mono.just(subscribeRequest))
                .filter(it -> it.getAssignment().getPartition() == topicPartition.partition())
                .flatMap(it -> stub.receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build())))
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record.getRecord().getOffset())
                .isEqualTo(6);
    }
}
