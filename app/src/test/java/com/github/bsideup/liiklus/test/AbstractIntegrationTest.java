package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import com.github.bsideup.liiklus.test.support.LocalStackContainer;
import com.linecorp.armeria.server.Server;
import io.grpc.ManagedChannelBuilder;
import lombok.val;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@SpringBootTest(
        classes = {Application.class, TestConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "armeria.port=0",
        }
)
public abstract class AbstractIntegrationTest {

    public static final int NUM_PARTITIONS = 32;

    // Generate a set of keys where each key goes to unique partition
    public static Set<String> PARTITION_UNIQUE_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .distinct(key -> Utils.toPositive(Utils.murmur2(key.getBytes())) % NUM_PARTITIONS)
            .take(NUM_PARTITIONS)
            .collect(Collectors.toSet())
            .block(Duration.ofSeconds(10));

    static {
        val localstack = new LocalStackContainer();

        val kafka = new KafkaContainer()
                .withEnv("KAFKA_NUM_PARTITIONS", NUM_PARTITIONS + "");

        Stream.of(kafka, localstack).parallel().forEach(GenericContainer::start);

        System.setProperty("kafka.bootstrapServers", kafka.getBootstrapServers());

        System.setProperty("dynamodb.positionsTable", "positions-" + UUID.randomUUID());
        System.getProperties().putAll(localstack.getProperties());
    }

    @Autowired
    private Server server;

    @Rule
    public TestName testName = new TestName();

    protected ReactorLiiklusServiceStub stub;

    @Before
    public void setUpAbstractIntegrationTest() throws Exception {
        stub = ReactorLiiklusServiceGrpc.newReactorStub(
                ManagedChannelBuilder.forTarget("localhost:" + server.activePort().get().localAddress().getPort())
                        .usePlaintext(true)
                        .keepAliveWithoutCalls(true)
                        .keepAliveTime(150, TimeUnit.SECONDS)
                        .build()
        );
    }
}
