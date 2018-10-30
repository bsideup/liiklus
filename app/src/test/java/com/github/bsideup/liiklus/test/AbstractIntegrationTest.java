package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import lombok.val;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.springframework.context.ApplicationContext;
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

    protected static LiiklusClient stub;

    protected static final ApplicationContext applicationContext;

    static {
        System.setProperty("server.port", "0");
        System.setProperty("grpc.enabled", "false");
        System.setProperty("plugins.dir", "../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        val args = Arrays.asList(
                "grpc.inProcessServerName=liiklus",
                "storage.positions.type=MEMORY",
                "storage.records.type=MEMORY",
                "rsocket.port=0"
        );

        applicationContext = Application.start(args.stream().map(it -> "--" + it).toArray(String[]::new));

        boolean useGrpc = false;
        if (useGrpc) {
            stub = new GRPCLiiklusClient(InProcessChannelBuilder.forName("liiklus").build());
        } else {
            val transport = TcpClientTransport.create(applicationContext.getBean(CloseableChannel.class).address());
            val rSocket = RSocketFactory.connect().transport(transport).start().block();
            stub = new RSocketLiiklusClient(rSocket);
        }

        Hooks.onOperatorDebug();
    }

    @Rule
    public TestName testName = new TestName();
}
