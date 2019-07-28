package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.apache.pulsar.client.util.MathUtils;
import org.springframework.context.ApplicationContext;
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

    static final ApplicationContext applicationContext;

    static {
        pulsar.start();
        System.setProperty("pulsar.serviceUrl", pulsar.getPulsarBrokerUrl());

        applicationContext = new ApplicationRunner("PULSAR", "MEMORY").run();
    }

    @Getter
    RecordsStorage target = applicationContext.getBean(RecordsStorage.class);

    @Getter
    String topic = UUID.randomUUID().toString();

    @SneakyThrows
    public PulsarRecordsStorageTest() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();

        pulsarAdmin.topics().createPartitionedTopic(topic, getNumberOfPartitions());
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