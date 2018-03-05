package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import com.github.bsideup.liiklus.test.support.LocalStackContainer;
import com.google.common.collect.Sets;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.kafka.common.utils.Utils;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(SpringRunner.class)
@ActiveProfiles(profiles = {"test", "exporter", "gateway"})
@SpringBootTest(
        classes = {Application.class, TestConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "grpc.inProcessServerName=liiklus",
        }
)
@ContextConfiguration(initializers = AbstractIntegrationTest.Initializer.class)
public abstract class AbstractIntegrationTest {

    public static final int NUM_PARTITIONS = 32;

    // Generate a set of keys where each key goes to unique partition
    public static Set<String> PARTITION_UNIQUE_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .distinct(AbstractIntegrationTest::getPartitionByKey)
            .take(NUM_PARTITIONS)
            .collect(Collectors.toSet())
            .block(Duration.ofSeconds(10));

    public static int getPartitionByKey(String key) {
        return Utils.toPositive(Utils.murmur2(key.getBytes())) % NUM_PARTITIONS;
    }

    private static LocalStackContainer localstack = new LocalStackContainer();

    private static KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_NUM_PARTITIONS", NUM_PARTITIONS + "");

    static {
        Stream.of(kafka, localstack).parallel().forEach(GenericContainer::start);

        System.setProperty("grpc.enabled", "false");
    }

    public static Set<String> getKafkaProperties() {
        return Sets.newHashSet(
                "kafka.bootstrapServers=" + kafka.getBootstrapServers()
        );
    }

    public static Set<String> getDynamoDBProperties() {
        return Sets.union(
                localstack.getProperties().entrySet().stream().map(it -> it.getKey() + "=" + it.getValue()).collect(Collectors.toSet()),
                Sets.newHashSet("dynamodb.positionsTable=positions")
        );
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertyValues.of(getKafkaProperties()).applyTo(applicationContext);
            TestPropertyValues.of(getDynamoDBProperties()).applyTo(applicationContext);
        }
    }

    @Rule
    public TestName testName = new TestName();

    protected ReactorLiiklusServiceStub stub = ReactorLiiklusServiceGrpc.newReactorStub(
            InProcessChannelBuilder.forName("liiklus").build()
    );
}
