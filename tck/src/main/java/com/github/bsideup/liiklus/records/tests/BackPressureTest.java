package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.val;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

public interface BackPressureTest extends RecordStorageTestSupport {

    @Test
    default void testSubscribeBackpressure() throws Exception {
        val numRecords = 20;
        val key = "key".getBytes();

        val offsetInfos = publishMany(key, numRecords);
        val partition = offsetInfos.get(0).getPartition();

        val recordFlux = subscribeToPartition(partition, "earliest").flatMap(RecordsStorage.PartitionSource::getPublisher);

        val initialRequest = 1;
        StepVerifier.create(recordFlux, initialRequest)
                .expectSubscription()
                .expectNextCount(1)
                .then(() -> publishMany(key, 10))
                .thenRequest(1)
                .expectNextCount(1)
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    default void testSubscribeWithoutRequest() throws Exception {
        val numRecords = 20;
        val key = "key".getBytes();

        val offsetInfos = publishMany(key, numRecords);
        val partition = offsetInfos.get(0).getPartition();

        val recordFlux = subscribeToPartition(partition, "earliest").flatMap(RecordsStorage.PartitionSource::getPublisher);

        val initialRequest = 0;
        StepVerifier.create(recordFlux, initialRequest)
                .expectSubscription()
                .then(() -> publishMany(key, 10))
                .expectNoEvent(Duration.ofSeconds(1))
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }
}
