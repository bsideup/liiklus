package com.github.bsideup.liiklus.plugins.example.support;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.ToStringConsumer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class AbstractIntegrationTest {

    protected static final ReactorLiiklusServiceStub stub;

    static {
        KafkaContainer kafka = new KafkaContainer();

        GenericContainer liiklus = new GenericContainer("bsideup/liiklus:0.3.0")
                .withNetwork(kafka.getNetwork())
                .withEnv("storage_positions_type", "MEMORY")
                .withEnv("kafka_bootstrapServers", kafka.getNetworkAliases().get(0) + ":9092")
                .withExposedPorts(6565)
                .withClasspathResourceMapping("/example-plugin.jar", "/app/plugins/example-plugin.jar", BindMode.READ_ONLY)
                .withLogConsumer(new ToStringConsumer() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        System.out.print("\uD83D\uDEA6 " + outputFrame.getUtf8String());
                    }
                });

        Stream.of(kafka, liiklus).parallel().forEach(GenericContainer::start);

        ManagedChannel channel = NettyChannelBuilder.forAddress(liiklus.getContainerIpAddress(), liiklus.getMappedPort(6565))
                .usePlaintext()
                .build();

        stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);
    }

    protected String topic = "test-topic-" + UUID.randomUUID();

    protected PublishReply publishRecord(String key, String value) {
        return stub.publish(Mono.just(PublishRequest.newBuilder()
                .setTopic(topic)
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build()
        )).block(Duration.ofSeconds(10));
    }

    protected Flux<Record> receiveRecords(String key) {
        return stub
                .subscribe(Mono.just(SubscribeRequest.newBuilder()
                        .setTopic(topic)
                        .setGroup(UUID.randomUUID().toString())
                        .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                        .build()
                ))
                .flatMap(it -> stub.receive(Mono.just(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build())))
                .map(ReceiveReply::getRecord)
                .filter(it -> key.equals(it.getKey().toStringUtf8()));
    }
}
