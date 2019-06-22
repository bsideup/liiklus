package com.example;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.SubscribeRequest.AutoOffsetReset;
import com.google.protobuf.ByteString;
import io.grpc.netty.NettyChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class Consumer {
    public static void main(String[] args) {
        // This variable should point to your Liiklus deployment (possible behind a Load Balancer)
        String liiklusTarget = getLiiklusTarget();

        var channel = NettyChannelBuilder.forTarget(liiklusTarget)
                .directExecutor()
                .usePlaintext(true)
                .build();

        var subscribeAction = SubscribeRequest.newBuilder()
                .setTopic("events-topic")
                .setGroup("my-group")
                .setAutoOffsetReset(AutoOffsetReset.EARLIEST)
                .build();

        var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

        // Send an event every second
        Flux.interval(Duration.ofSeconds(1))
                .onBackpressureDrop()
                .concatMap(it -> stub.publish(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeAction.getTopic())
                                .setKey(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                .setValue(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                .build()
                ))
                .subscribe();

        // Consume the events
        Function<Integer, Function<ReceiveReply.Record, Publisher<?>>> businessLogic = partition -> record -> {
            log.info("Processing record from partition {} offset {}", partition, record.getOffset());

            // simulate processing
            return Mono.delay(Duration.ofMillis(200));
        };

        stub
                .subscribe(subscribeAction)
                .filter(it -> it.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT)
                .map(SubscribeReply::getAssignment)
                .doOnNext(assignment -> log.info("Assigned to partition {}", assignment.getPartition()))
                .flatMap(assignment -> stub
                        // Start receiving the events from a partition
                        .receive(ReceiveRequest.newBuilder().setAssignment(assignment).build())
                        .window(1000) // ACK every 1000th record
                        .concatMap(
                                batch -> batch
                                        .map(ReceiveReply::getRecord)
                                        .delayUntil(businessLogic.apply(assignment.getPartition()))
                                        .sample(Duration.ofSeconds(5)) // ACK every 5 seconds
                                        .onBackpressureLatest()
                                        .delayUntil(record -> {
                                            log.info("ACKing partition {} offset {}", assignment.getPartition(), record.getOffset());
                                            return stub.ack(
                                                    AckRequest.newBuilder()
                                                            .setAssignment(assignment)
                                                            .setOffset(record.getOffset())
                                                            .build()
                                            );
                                        }),
                                1
                        )
                )
                .blockLast();
    }

    private static String getLiiklusTarget() {
        var kafka = new KafkaContainer()
                .withEnv("KAFKA_NUM_PARTITIONS", "4");

        GenericContainer liiklus = new GenericContainer<>("bsideup/liiklus:0.1.8")
                .withNetwork(kafka.getNetwork())
                .withExposedPorts(6565)
                .withEnv("kafka_bootstrapServers", kafka.getNetworkAliases().get(0) + ":9093")
                .withEnv("storage_positions_type", "MEMORY"); // Fine for testing, NOT FINE I WARNED YOU for production :D

        Stream.of(kafka, liiklus).parallel().forEach(GenericContainer::start);

        log.info("Containers started");

        return String.format("%s:%d", liiklus.getContainerIpAddress(), liiklus.getFirstMappedPort());
    }
}