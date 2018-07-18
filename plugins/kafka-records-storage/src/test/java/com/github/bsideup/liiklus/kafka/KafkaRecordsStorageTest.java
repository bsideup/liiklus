package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.testcontainers.containers.KafkaContainer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.nio.ByteBuffer;
import java.util.UUID;

public class KafkaRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 32;

    private static final KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_NUM_PARTITIONS", NUM_OF_PARTITIONS + "")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

    static {
        kafka.start();
    }

    @Getter
    RecordsStorage target = new KafkaRecordsStorage(
            kafka.getBootstrapServers(),
            KafkaSender.create(
                    SenderOptions.<ByteBuffer, ByteBuffer>create()
                            .stopOnError(false)
                            .producerProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                            .producerProperty(ProducerConfig.CLIENT_ID_CONFIG, "liiklus-" + UUID.randomUUID().toString())
                            .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class)
                            .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class)
            )
    );

    @Getter
    String topic = UUID.randomUUID().toString();

    @Override
    public int getNumberOfPartitions() {
        return NUM_OF_PARTITIONS;
    }
}