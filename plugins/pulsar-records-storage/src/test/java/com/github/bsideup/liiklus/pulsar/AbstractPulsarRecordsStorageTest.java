package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.support.DisabledUntil;
import lombok.Getter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

abstract class AbstractPulsarRecordsStorageTest implements RecordStorageTests {

    static final PulsarContainer pulsar = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.5.0"))
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
    @DisabledUntil(value = "2021-09-01", comment = "#180 - Pulsar should fix the way seek works, not disconnecting consumers (apache/pulsar/pull/5022)")
    public void shouldAlwaysUseEarliestOffsetOnEmptyOffsetsInTheInitialProvider() {
        RecordStorageTests.super.shouldAlwaysUseEarliestOffsetOnEmptyOffsetsInTheInitialProvider();
    }

    @Test
    void shouldPreferEventTimeOverPublishTime() throws Exception {
        var topic = getTopic();
        var eventTimestamp = Instant.now().minusSeconds(1000).truncatedTo(ChronoUnit.MILLIS);

        int partition;
        try (
                var pulsarClient = PulsarClient.builder()
                        .serviceUrl(pulsar.getPulsarBrokerUrl())
                        .build()
        ) {
            var messageId = pulsarClient.newProducer()
                    .topic(topic)
                    .create()
                    .newMessage()
                    .value("hello".getBytes())
                    .eventTime(eventTimestamp.toEpochMilli())
                    .send();

            partition = ((MessageIdImpl) messageId).getPartitionIndex();
        }

        var record = subscribeToPartition(partition)
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record).satisfies(it -> {
            assertThat(it.getTimestamp()).isEqualTo(eventTimestamp);
        });
    }
}
