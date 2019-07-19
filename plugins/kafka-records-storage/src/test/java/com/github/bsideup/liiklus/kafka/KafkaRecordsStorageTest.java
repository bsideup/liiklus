package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import org.apache.kafka.common.utils.Utils;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KafkaRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 4;

    // Generate a set of keys where each key goes to unique partition
    public static Map<Integer, String> PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .scanWith(
                    () -> new HashMap<Integer, String>(),
                    (acc, it) -> {
                        acc.put(Utils.toPositive(Utils.murmur2(it.getBytes())) % NUM_OF_PARTITIONS, it);
                        return acc;
                    }
            )
            .filter(it -> it.size() == NUM_OF_PARTITIONS)
            .blockFirst(Duration.ofSeconds(10));

    private static final KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_NUM_PARTITIONS", NUM_OF_PARTITIONS + "");

    static {
        kafka.start();
    }

    @Getter
    RecordsStorage target = new KafkaRecordsStorage(
            kafka.getBootstrapServers(), new HashMap<>()
    );

    @Getter
    String topic = UUID.randomUUID().toString();

    @Override
    public int getNumberOfPartitions() {
        return NUM_OF_PARTITIONS;
    }

    @Override
    public String keyByPartition(int partition) {
        return PARTITION_KEYS.get(partition);
    }
}