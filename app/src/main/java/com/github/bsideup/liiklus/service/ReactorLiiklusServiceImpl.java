package com.github.bsideup.liiklus.service;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.github.bsideup.liiklus.source.KafkaSource;
import com.github.bsideup.liiklus.source.KafkaSource.KafkaRecord;
import com.github.bsideup.liiklus.source.KafkaSource.Subscription;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.lognet.springboot.grpc.GRpcService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
@GRpcService
public class ReactorLiiklusServiceImpl extends ReactorLiiklusServiceGrpc.LiiklusServiceImplBase {

    ConcurrentMap<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    ConcurrentMap<String, ConcurrentMap<Integer, Flux<KafkaRecord>>> sources = new ConcurrentHashMap<>();

    KafkaSource kafkaSource;

    @Override
    public Mono<PublishReply> publish(Mono<PublishRequest> requestMono) {
        return requestMono
                .flatMap(request -> kafkaSource.publish(
                        request.getTopic(),
                        request.getKey().asReadOnlyByteBuffer(),
                        request.getValue().asReadOnlyByteBuffer()
                ))
                .then(Mono.just(PublishReply.getDefaultInstance()));
    }

    @Override
    public Flux<SubscribeReply> subscribe(Mono<SubscribeRequest> requestFlux) {
        return requestFlux
                .flatMapMany(subscribe -> {
                    Map<String, Object> props = new HashMap<>();
                    String groupId = subscribe.getGroup();
                    String topic = subscribe.getTopic();
                    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

                    switch (subscribe.getAutoOffsetReset()) {
                        case EARLIEST:
                            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                            break;
                        case LATEST:
                            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                            break;
                        case UNRECOGNIZED:
                            // ignore
                    }

                    Subscription subscription = kafkaSource.subscribe(props, topic);

                    String sessionId = UUID.randomUUID().toString();

                    subscriptions.put(sessionId, subscription);

                    return Flux.from(subscription.getPublisher())
                            .<SubscribeReply>handle((group, sink) -> {
                                int partition = group.getGroup();

                                ConcurrentMap<Integer, Flux<KafkaRecord>> sourcesByPartition = sources
                                        .computeIfAbsent(sessionId, __ -> new ConcurrentHashMap<>());

                                sourcesByPartition.put(
                                        partition,
                                        Flux.from(group)
                                                .log("partition-" + partition, Level.WARNING, SignalType.ON_ERROR)
                                                .retry()
                                                .doFinally(__ -> {
                                                    sourcesByPartition.remove(partition);
                                                    sink.complete();
                                                })
                                );

                                sink.next(
                                        SubscribeReply.newBuilder()
                                                .setAssignment(Assignment.newBuilder()
                                                        .setPartition(partition)
                                                        .setSessionId(sessionId)
                                                )
                                                .build()
                                );
                            })
                            .doFinally(__ -> subscriptions.remove(sessionId, subscription));
                })
                .log("subscribe", Level.WARNING, SignalType.ON_ERROR);
    }

    @Override
    public Flux<ReceiveReply> receive(Mono<ReceiveRequest> requestMono) {
        return requestMono
                .flatMapMany(request -> {
                    String sessionId = request.getAssignment().getSessionId();
                    int partition = request.getAssignment().getPartition();
                    // TODO auto ack to the last known offset
                    long lastKnownOffset = request.getLastKnownOffset();

                    Flux<KafkaRecord> source = sources.get(sessionId).get(partition);

                    if (source == null) {
                        log.warn("Source is null, returning empty Publisher. Request: {}", request);
                        return Mono.empty();
                    }

                    return source
                            .map(consumerRecord -> ReceiveReply.newBuilder()
                                    .setRecord(
                                            Record.newBuilder()
                                                    .setOffset(consumerRecord.getOffset())
                                                    .setKey(ByteString.copyFrom(consumerRecord.getKey()))
                                                    .setValue(ByteString.copyFrom(consumerRecord.getValue()))
                                                    .setTimestamp(Timestamp.newBuilder()
                                                            .setSeconds(consumerRecord.getTimestamp().getEpochSecond())
                                                            .setNanos(consumerRecord.getTimestamp().getNano())
                                                    )
                                    )
                                    .build()
                            );
                })
                .log("receive", Level.WARNING, SignalType.ON_ERROR);
    }

    @Override
    public Mono<Empty> ack(Mono<AckRequest> request) {
        return request
                .flatMap(ack -> {
                    Subscription subscription = subscriptions.get(ack.getAssignment().getSessionId());

                    if (subscription == null) {
                        log.warn("Subscription is null, returning empty Publisher. Request: {}", ack);
                        return Mono.empty();
                    }

                    return subscription.acknowledge(ack.getAssignment().getPartition(), ack.getOffset());
                })
                .then(Mono.just(Empty.getDefaultInstance()))
                .log("ack", Level.WARNING, SignalType.ON_ERROR);
    }
}
