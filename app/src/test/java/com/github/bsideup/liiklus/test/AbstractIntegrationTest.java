package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import com.github.bsideup.liiklus.test.support.LocalStackContainer;
import com.google.common.collect.Sets;
import io.grpc.inprocess.InProcessChannelBuilder;
import lombok.val;
import org.apache.kafka.common.utils.Utils;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public abstract class AbstractIntegrationTest {

    public static final int NUM_PARTITIONS = 32;

    // Generate a set of keys where each key goes to unique partition
    public static Map<Integer, String> PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .scanWith(
                    () -> new HashMap<Integer, String>(),
                    (acc, it) -> {
                        acc.put(getPartitionByKey(it), it);
                        return acc;
                    }
            )
            .filter(it -> it.size() == NUM_PARTITIONS)
            .blockFirst(Duration.ofSeconds(10));

    public static Set<String> PARTITION_UNIQUE_KEYS = new HashSet<>(PARTITION_KEYS.values());

    public static int getPartitionByKey(String key) {
        return Utils.toPositive(Utils.murmur2(key.getBytes())) % NUM_PARTITIONS;
    }

    private static LocalStackContainer localstack = new LocalStackContainer();

    protected static KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_NUM_PARTITIONS", NUM_PARTITIONS + "");

    protected static ReactorLiiklusServiceStub stub = ReactorLiiklusServiceGrpc.newReactorStub(
            InProcessChannelBuilder.forName("liiklus").build()
    );

    static {
        Stream.of(kafka, localstack).parallel().forEach(GenericContainer::start);

        System.getProperties().putAll(localstack.getProperties());

        System.setProperty("server.port", "0");
        System.setProperty("grpc.enabled", "false");
        System.setProperty("plugins.dir", "../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        val args = new ArrayList<String>();
        args.add("grpc.inProcessServerName=liiklus");
        args.add("dynamodb.autoCreateTable=true");
        args.addAll(getKafkaProperties());
        args.addAll(getDynamoDBProperties());

        Application.start(args.stream().map(it -> "--" + it).toArray(String[]::new));

        Hooks.onOperatorDebug();
    }

    public static Set<String> getKafkaProperties() {
        return Sets.newHashSet(
                "kafka.bootstrapServers=" + kafka.getBootstrapServers()
        );
    }

    public static Set<String> getDynamoDBProperties() {
        return Sets.newHashSet(
                "dynamodb.positionsTable=positions"
        );
    }

    @Rule
    public TestName testName = new TestName();
}
