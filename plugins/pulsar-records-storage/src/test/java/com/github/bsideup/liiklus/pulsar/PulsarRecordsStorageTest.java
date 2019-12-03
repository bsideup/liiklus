package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage.PartitionSource;
import com.github.bsideup.liiklus.support.DisabledUntil;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.apache.pulsar.client.util.MathUtils;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.PulsarContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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

        applicationContext = new ApplicationRunner("PULSAR", "MEMORY")
                .withProperty("pulsar.serviceUrl", pulsar.getPulsarBrokerUrl())
                .run();
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

    @Override
    @Test
    @DisabledUntil(value = "2020-01-01", comment = "#180 - Pulsar should fix the way seek works, not disconnecting consumers (apache/pulsar/pull/5022)")
    public void shouldAlwaysUseEarliestOffsetOnEmptyOffsetsInTheInitialProvider() {
        RecordStorageTests.super.shouldAlwaysUseEarliestOffsetOnEmptyOffsetsInTheInitialProvider();
    }

    @Test
    void shouldPreferEventTimeOverPublishTime() throws Exception {
        var topic = getTopic();
        var partition = 0;
        var key = keyByPartition(partition);
        var eventTimestamp = Instant.now().minusSeconds(1000).truncatedTo(ChronoUnit.MILLIS);

        try (
                var pulsarClient = PulsarClient.builder()
                        .serviceUrl(pulsar.getPulsarBrokerUrl())
                        .build()
        ) {
            pulsarClient.newProducer()
                    .topic(topic)
                    .hashingScheme(HashingScheme.Murmur3_32Hash)
                    .create()
                    .newMessage()
                    .key(key)
                    .value("hello".getBytes())
                    .eventTime(eventTimestamp.toEpochMilli())
                    .send();
        }

        var record = subscribeToPartition(partition)
                .flatMap(PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record).satisfies(it -> {
            assertThat(it.getTimestamp()).isEqualTo(eventTimestamp);
        });
    }
}