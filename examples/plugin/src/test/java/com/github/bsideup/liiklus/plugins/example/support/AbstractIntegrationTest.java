package com.github.bsideup.liiklus.plugins.example.support;

import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.google.protobuf.ByteString;
import io.grpc.netty.NettyChannelBuilder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.ToStringConsumer;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.UUID;

public abstract class AbstractIntegrationTest {

    protected static final LiiklusClient client;

    static {
        LiiklusContainer liiklus = new LiiklusContainer("0.7.0")
                .withEnv("storage_records_type", "MEMORY")
                .withClasspathResourceMapping("/example-plugin-0.0.1-SNAPSHOT.jar", "/app/plugins/example-plugin.jar", BindMode.READ_ONLY)
                .withLogConsumer(new ToStringConsumer() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        System.out.print("\uD83D\uDEA6 " + outputFrame.getUtf8String());
                    }
                });

        liiklus.start();

        client = new GRPCLiiklusClient(
                NettyChannelBuilder.forTarget(liiklus.getTarget())
                        .usePlaintext()
                        .build()
        );
    }

    protected String topic = "test-topic-" + UUID.randomUUID();

    protected PublishReply publishRecord(String key, String value) {
        return client.publish(PublishRequest.newBuilder()
                .setTopic(topic)
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build()
        ).block(Duration.ofSeconds(10));
    }

    protected Flux<Record> receiveRecords(String key) {
        return client
                .subscribe(SubscribeRequest.newBuilder()
                        .setTopic(topic)
                        .setGroup(UUID.randomUUID().toString())
                        .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                        .build()
                )
                .flatMap(it -> client.receive(ReceiveRequest.newBuilder().setAssignment(it.getAssignment()).build()))
                .map(ReceiveReply::getRecord)
                .filter(it -> key.equals(it.getKey().toStringUtf8()));
    }
}
