package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.pf4j.PluginManager;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
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
        System.setProperty("plugins.dir", "../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        var args = Arrays.asList(
                "storage.positions.type=MEMORY",
                "storage.records.type=MEMORY",
                "rsocket.port=0",
                "grpc.port=0",
                "server.port=0"
        );

        applicationContext = Application.start(args.stream().map(it -> "--" + it).toArray(String[]::new));
        var pluginManager = applicationContext.getBean(PluginManager.class);

        boolean useGrpc = false;
        if (useGrpc) {
            try {
                var classLoader = pluginManager.getPluginClassLoader("grpc-transport");
                var serverClazz = classLoader.loadClass(Server.class.getName());
                var getPortMethod = serverClazz.getDeclaredMethod("getPort");
                var server = applicationContext.getBean(serverClazz);

                stub = new GRPCLiiklusClient(
                        ManagedChannelBuilder
                                .forAddress("localhost", (int) getPortMethod.invoke(server))
                                .usePlaintext()
                                .build()
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                var classLoader = pluginManager.getPluginClassLoader("rsocket-transport");
                var closeableChannelClass = classLoader.loadClass(CloseableChannel.class.getName());
                var addressMethod = closeableChannelClass.getDeclaredMethod("address");
                var closeableChannel = applicationContext.getBean(closeableChannelClass);

                stub = new RSocketLiiklusClient(
                        RSocketFactory.connect()
                                .transport(
                                        TcpClientTransport.create((InetSocketAddress) addressMethod.invoke(closeableChannel))
                                )
                                .start().block()
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        Hooks.onOperatorDebug();
    }

    @Rule
    public TestName testName = new TestName();
}
