package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import io.grpc.inprocess.InProcessChannelBuilder;
import lombok.val;
import org.junit.Rule;
import org.junit.rules.TestName;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

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
        return Math.abs(ByteBuffer.wrap(key.getBytes()).hashCode()) % NUM_PARTITIONS;
    }

    protected static ReactorLiiklusServiceStub stub = ReactorLiiklusServiceGrpc.newReactorStub(
            InProcessChannelBuilder.forName("liiklus").build()
    );

    static {
        System.setProperty("server.port", "0");
        System.setProperty("grpc.enabled", "false");
        System.setProperty("plugins.dir", "../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        val args = Arrays.asList(
                "grpc.inProcessServerName=liiklus",
                "storage.positions.type=MEMORY",
                "storage.records.type=MEMORY"
        );

        Application.start(args.stream().map(it -> "--" + it).toArray(String[]::new));

        Hooks.onOperatorDebug();
    }

    @Rule
    public TestName testName = new TestName();
}
