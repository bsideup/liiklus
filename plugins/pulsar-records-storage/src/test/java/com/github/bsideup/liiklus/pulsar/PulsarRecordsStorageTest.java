package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.apache.pulsar.client.util.MathUtils;
import org.testcontainers.containers.PulsarContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PulsarRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 4;

    // Generate a set of keys where each key goes to unique partition
    public static Map<Integer, String> PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .scanWith(
                    () -> new HashMap<Integer, String>(),
                    (acc, it) -> {
                        acc.put(MathUtils.signSafeMod(Murmur3_32Hash.getInstance().makeHash(it), NUM_OF_PARTITIONS), it);
                        return acc;
                    }
            )
            .filter(it -> it.size() == NUM_OF_PARTITIONS)
            .blockFirst(Duration.ofSeconds(10));

    private static final PulsarContainer pulsar = new PulsarContainer();

    static {
        pulsar.start();
    }

    @Getter
    RecordsStorage target;

    @Getter
    String topic = UUID.randomUUID().toString();

    @SneakyThrows
    public PulsarRecordsStorageTest() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();

        pulsarAdmin.topics().createPartitionedTopic(topic, getNumberOfPartitions());

        this.target = new PulsarRecordsStorage(
                PulsarClient.builder()
                        .serviceUrl(pulsar.getPulsarBrokerUrl())
                        .build()
        );
    }

    @Override
    public String keyByPartition(int partition) {
        return PARTITION_KEYS.get(partition);
    }

    @Override
    public int getNumberOfPartitions() {
        return NUM_OF_PARTITIONS;
    }
}