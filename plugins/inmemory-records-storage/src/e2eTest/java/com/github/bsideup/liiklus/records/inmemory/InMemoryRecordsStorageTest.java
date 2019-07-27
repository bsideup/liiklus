package com.github.bsideup.liiklus.records.inmemory;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import org.pf4j.PluginManager;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class InMemoryRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 32;

    // Generate a set of keys where each key goes to unique partition
    static final Map<Integer, String> PARTITION_KEYS;

    static final ApplicationContext applicationContext;

    static {
        applicationContext = new ApplicationRunner("MEMORY", "MEMORY").run();

        var pluginManager = applicationContext.getBean(PluginManager.class);

        var pluginClassLoader = pluginManager.getPluginClassLoader("inmemory-records-storage");
        try {
            var clazz = pluginClassLoader.loadClass("com.github.bsideup.liiklus.records.inmemory.InMemoryRecordsStorage");
            var partitionByKeyMethod = clazz.getDeclaredMethod("partitionByKey", String.class, int.class);

            PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
                    .repeat()
                    .scanWith(
                            () -> new HashMap<Integer, String>(),
                            (acc, it) -> {
                                try {
                                    var partition = (int) partitionByKeyMethod.invoke(null, it, NUM_OF_PARTITIONS);
                                    acc.put(partition, it);
                                    return acc;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    )
                    .filter(it -> it.size() == NUM_OF_PARTITIONS)
                    .blockFirst(Duration.ofSeconds(10));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    RecordsStorage target = applicationContext.getBean(RecordsStorage.class);

    @Getter
    String topic = UUID.randomUUID().toString();

    @Override
    public String keyByPartition(int partition) {
        return PARTITION_KEYS.get(partition);
    }

    @Override
    public int getNumberOfPartitions() {
        return NUM_OF_PARTITIONS;
    }
}