package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.pf4j.PluginManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public abstract class AbstractIntegrationTest {

    public static final LiiklusEvent LIIKLUS_EVENT_EXAMPLE = LiiklusEvent.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setType("com.example.event")
            .setSource("/tests")
            .setDataContentType("application/json")
            .putExtensions("comexampleextension1", "foo")
            .putExtensions("comexampleextension2", "bar")
            .setTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .buildPartial();

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

    protected static final ProcessorPluginMock processorPluginMock = new ProcessorPluginMock();

    static {
        System.setProperty("plugins.dir", "../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        var args = Arrays.asList(
                "storage.positions.type=MEMORY",
                "storage.records.type=MEMORY",
                "rsocket.port=0",
                "grpc.port=0",
                "server.port=0"
        ).stream().map(it -> "--" + it).toArray(String[]::new);

        var springApplication = Application.createSpringApplication(args);
        springApplication.addInitializers(applicationContext -> {
            ((GenericApplicationContext) applicationContext).registerBean(
                    "processorPluginMock",
                    ProcessorPluginMock.class,
                    () -> processorPluginMock
            );
        });
        applicationContext = springApplication.run(args);
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

                var transport = TcpClientTransport.create((InetSocketAddress) addressMethod.invoke(closeableChannel));

                stub = new RSocketLiiklusClient(
                        RSocketConnector.connectWith(transport).block()
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        Hooks.onOperatorDebug();
    }

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUpAbstractIntegrationTest() throws Exception {
        processorPluginMock.getPreProcessors().clear();
        processorPluginMock.getPostProcessors().clear();
    }
}
