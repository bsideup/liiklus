package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.support.DisabledUntil;
import lombok.Getter;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.PulsarContainer;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

abstract class AbstractPulsarRecordsStorageTest implements RecordStorageTests {

    static final PulsarContainer pulsar = new PulsarContainer("2.5.0")
            .withReuse(true);

    private static final ApplicationContext applicationContext;

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

    @Override
    @Test
    @DisabledUntil(value = "2020-03-01", comment = "#180 - Pulsar should fix the way seek works, not disconnecting consumers (apache/pulsar/pull/5022)")
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
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record).satisfies(it -> {
            assertThat(it.getTimestamp()).isEqualTo(eventTimestamp);
        });
    }
}
