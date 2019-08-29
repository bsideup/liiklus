package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.DirectProcessor;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public interface SubscribeTest extends RecordStorageTestSupport {

    @Test
    default void testSubscribeWithEarliest() throws Exception {
        var numRecords = 5;
        var key = UUID.randomUUID().toString().getBytes();

        var offsetInfos = publishMany(key, numRecords);

        var partition = offsetInfos.get(0).getPartition();

        var disposeAll = DirectProcessor.<Boolean>create();

        try {
            var recordsSoFar = new ArrayList<RecordsStorage.Record>();

            subscribeToPartition(partition, "earliest")
                    .flatMap(RecordsStorage.PartitionSource::getPublisher)
                    .takeUntilOther(disposeAll)
                    .subscribe(recordsSoFar::add);

            await.untilAsserted(() -> {
                assertThat(recordsSoFar).hasSize(numRecords);
            });

            publish(key, UUID.randomUUID().toString().getBytes());

            await.untilAsserted(() -> {
                assertThat(recordsSoFar).hasSize(numRecords + 1);
            });
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    default void testSubscribeWithLatest() throws Exception {
        var key = UUID.randomUUID().toString().getBytes();

        var offsetInfos = publishMany(key, 5);

        var partition = offsetInfos.get(0).getPartition();

        var disposeAll = DirectProcessor.<Boolean>create();

        try {
            var recordsSoFar = new ArrayList<RecordsStorage.Record>();
            var assigned = new AtomicBoolean(false);

            subscribeToPartition(partition, "latest")
                    .doOnNext(__ -> assigned.set(true))
                    .flatMap(RecordsStorage.PartitionSource::getPublisher)
                    .takeUntilOther(disposeAll)
                    .subscribe(recordsSoFar::add);

            await.untilTrue(assigned);

            var envelope = createEnvelope(key);
            var offsetInfo = publish(envelope);

            await.untilAsserted(() -> {
                assertThat(recordsSoFar)
                        .hasSize(1)
                        .allSatisfy(it -> {
                            assertThat(it.getEnvelope()).as("envelope").isEqualTo(envelope);
                            assertThat(it.getPartition()).as("partition").isEqualTo(offsetInfo.getPartition());
                            assertThat(it.getOffset()).as("offset").isEqualTo(offsetInfo.getOffset());
                        });
            });
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    default void testSubscribeSorting() {
        var numRecords = 5;

        var offsetInfos = publishMany("key".getBytes(), numRecords);
        var partition = offsetInfos.get(0).getPartition();

        var records = subscribeToPartition(partition, "earliest")
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .take(numRecords)
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(records)
                .isSortedAccordingTo(Comparator.comparingLong(RecordsStorage.Record::getOffset));
    }

    @Test
    default void testInitialOffsets() throws Exception {
        var offsetInfos = publishMany("key".getBytes(), 10);
        var offsetInfo = offsetInfos.get(7);
        var partition = offsetInfo.getPartition();
        var position = offsetInfo.getOffset();

        var receivedRecords = subscribeToPartition(partition, Optional.of("earliest"), () -> CompletableFuture.completedFuture(Collections.singletonMap(partition, position)))
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .take(3)
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(receivedRecords).extracting(RecordsStorage.Record::getOffset).containsExactly(
                offsetInfos.get(7).getOffset(),
                offsetInfos.get(8).getOffset(),
                offsetInfos.get(9).getOffset()
        );
    }

    @Test
    default void testNullKey() throws Exception {
        var topic = getTopic();
        var offsetInfo = publish(new RecordsStorage.Envelope(
                topic,
                null,
                ByteBuffer.wrap("hello".getBytes())
        ));
        int partition = offsetInfo.getPartition();

        var record = subscribeToPartition(partition)
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record)
                .isNotNull()
                .satisfies(it -> {
                    assertThat(it.getOffset()).isEqualTo(offsetInfo.getOffset());
                });
    }

    @Test
    default void testValidTimestamp() throws Exception {
        var topic = getTopic();
        var offsetInfo = publish(new RecordsStorage.Envelope(
                topic,
                null,
                ByteBuffer.wrap("hello".getBytes())
        ));
        int partition = offsetInfo.getPartition();

        var record = subscribeToPartition(partition)
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record)
                .isNotNull()
                .satisfies(it -> {
                    assertThat(it.getTimestamp()).isAfter(Instant.ofEpochMilli(0));
                });
    }
}
