package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import org.pf4j.PluginManager;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KafkaRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 4;

    // Generate a set of keys where each key goes to unique partition
    public static final Map<Integer, String> PARTITION_KEYS;

    private static final KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_NUM_PARTITIONS", NUM_OF_PARTITIONS + "");

    static final ApplicationContext applicationContext;

    static {
        kafka.start();

        System.setProperty("server.port", "0");
        System.setProperty("rsocket.enabled", "false");
        System.setProperty("grpc.enabled", "false");

        System.setProperty("plugins.dir", "../../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        System.setProperty("storage.positions.type", "MEMORY");

        System.setProperty("storage.records.type", "KAFKA");
        System.setProperty("kafka.bootstrapServers", kafka.getBootstrapServers());

        applicationContext = Application.start(new String[0]);

        try {
            var pluginClassLoader = applicationContext.getBean(PluginManager.class).getPluginClassLoader("kafka-records-storage");
            var utilsClass = pluginClassLoader.loadClass("org.apache.kafka.common.utils.Utils");
            var murmur2Method = utilsClass.getDeclaredMethod("murmur2", byte[].class);

            PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
                    .repeat()
                    .scanWith(
                            () -> new HashMap<Integer, String>(),
                            (acc, it) -> {
                                try {
                                    int hash = (int) murmur2Method.invoke(null, (Object) it.getBytes());
                                    hash = hash & 0x7fffffff;
                                    acc.put(hash % NUM_OF_PARTITIONS, it);
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
    public int getNumberOfPartitions() {
        return NUM_OF_PARTITIONS;
    }

    @Override
    public String keyByPartition(int partition) {
        return PARTITION_KEYS.get(partition);
    }
}