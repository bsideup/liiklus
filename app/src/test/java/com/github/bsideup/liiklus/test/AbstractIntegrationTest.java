package com.github.bsideup.liiklus.test;


import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import java.util.stream.Stream;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "grpc.enabled=false",
                "grpc.inProcessServerName=liiklus",
        }
)
public abstract class AbstractIntegrationTest {

    protected static final ManagedChannel channel = InProcessChannelBuilder.forName("liiklus")
            .build();

    static {
        KafkaContainer kafka = new KafkaContainer();

        Stream.of(kafka).parallel().forEach(GenericContainer::start);

        System.setProperty("kafka.bootstrapServers", kafka.getBootstrapServers());
    }

    @Rule
    public TestName testName = new TestName();

    protected ReactorLiiklusServiceStub stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);
}
