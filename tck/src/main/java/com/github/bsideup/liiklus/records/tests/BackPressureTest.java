package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

public interface BackPressureTest extends RecordStorageTestSupport {

    @Test
    default void testSubscribeBackpressure() throws Exception {
        var numRecords = 20;
        var key = "key".getBytes();

        var offsetInfos = publishMany(key, numRecords);
        var partition = offsetInfos.get(0).getPartition();

        var recordFlux = subscribeToPartition(partition, "earliest").flatMap(RecordsStorage.PartitionSource::getPublisher);

        var initialRequest = 1;
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
        var numRecords = 20;
        var key = "key".getBytes();

        var offsetInfos = publishMany(key, numRecords);
        var partition = offsetInfos.get(0).getPartition();

        var recordFlux = subscribeToPartition(partition, "earliest").flatMap(RecordsStorage.PartitionSource::getPublisher);

        var initialRequest = 0;
        StepVerifier.create(recordFlux, initialRequest)
                .expectSubscription()
                .then(() -> publishMany(key, 10))
                .expectNoEvent(Duration.ofSeconds(1))
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }
}
