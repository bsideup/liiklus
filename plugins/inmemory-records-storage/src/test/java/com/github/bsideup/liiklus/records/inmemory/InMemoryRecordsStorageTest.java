package com.github.bsideup.liiklus.records.inmemory;

import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class InMemoryRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 4;

    // Generate a set of keys where each key goes to unique partition
    public static Map<Integer, String> PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .scanWith(
                    () -> new HashMap<Integer, String>(),
                    (acc, it) -> {
                        acc.put(InMemoryRecordsStorage.partitionByKey(it, NUM_OF_PARTITIONS), it);
                        return acc;
                    }
            )
            .filter(it -> it.size() == NUM_OF_PARTITIONS)
            .blockFirst(Duration.ofSeconds(10));

    @Getter
    RecordsStorage target;

    @Getter
    String topic = UUID.randomUUID().toString();

    @SneakyThrows
    public InMemoryRecordsStorageTest() {
        this.target = new InMemoryRecordsStorage(NUM_OF_PARTITIONS);
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
    public void testMultipleGroups() throws Exception {
        RecordStorageTests.super.testMultipleGroups();
    }

    @Override
    @Test
    public void testExclusiveRecordDistribution() throws Exception {
        RecordStorageTests.super.testExclusiveRecordDistribution();
    }
}