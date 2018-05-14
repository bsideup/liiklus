package com.github.bsideup.liiklus.service;

import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.RecordPostProcessorChain;
import com.github.bsideup.liiklus.config.RecordPreProcessorChain;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;
import com.github.bsideup.liiklus.records.RecordsStorage.Record;
import com.github.bsideup.liiklus.records.RecordsStorage.Subscription;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
@GRpcService
@GatewayProfile
public class ReactorLiiklusServiceImpl extends ReactorLiiklusServiceGrpc.LiiklusServiceImplBase {

    ConcurrentMap<String, StoredSubscription> subscriptions = new ConcurrentHashMap<>();

    ConcurrentMap<String, ConcurrentMap<Integer, Flux<Record>>> sources = new ConcurrentHashMap<>();

    RecordsStorage recordsStorage;

    PositionsStorage positionsStorage;

    RecordPreProcessorChain recordPreProcessorChain;

    RecordPostProcessorChain recordPostProcessorChain;

    @Override
    public Mono<PublishReply> publish(Mono<PublishRequest> requestMono) {
        return requestMono
                .map(request -> new Envelope(
                        request.getTopic(),
                        request.getKey().asReadOnlyByteBuffer(),
                        request.getValue().asReadOnlyByteBuffer()
                ))
                .transform(mono -> {
                    for (RecordPreProcessor processor : recordPreProcessorChain.getAll()) {
                        mono = mono.flatMap(envelope -> {
                            try {
                                return Mono.fromCompletionStage(processor.preProcess(envelope));
                            } catch (Throwable e) {
                                return Mono.error(new RuntimeException(processor + " failed", e));
                            }
                        });
                    }
                    return mono;
                })
                .flatMap(envelope -> Mono.fromCompletionStage(recordsStorage.publish(envelope)))
                .map(it -> PublishReply.newBuilder()
                        .setTopic(it.getTopic())
                        .setPartition(it.getPartition())
                        .setOffset(it.getOffset())
                        .build()
                )
                .log("publish", Level.SEVERE, SignalType.ON_ERROR);
    }

    @Override
    public Flux<SubscribeReply> subscribe(Mono<SubscribeRequest> requestFlux) {
        return requestFlux
                .flatMapMany(subscribe -> {
                    String groupId = subscribe.getGroup();
                    String topic = subscribe.getTopic();

                    Optional<String> autoOffsetReset;
                    switch (subscribe.getAutoOffsetReset()) {
                        case EARLIEST:
                            autoOffsetReset = Optional.of("earliest");
                            break;
                        case LATEST:
                            autoOffsetReset = Optional.of("latest");
                            break;
                        default:
                            autoOffsetReset = Optional.empty();
                    }

                    Subscription subscription = recordsStorage.subscribe(topic, groupId, autoOffsetReset);

                    String sessionId = UUID.randomUUID().toString();

                    subscriptions.put(sessionId, new StoredSubscription(subscription, topic, groupId));

                    ConcurrentMap<Integer, Flux<Record>> sourcesByPartition = sources
                            .computeIfAbsent(sessionId, __ -> new ConcurrentHashMap<>());

                    return Flux.from(subscription.getPublisher())
                            .map(group -> {
                                int partition = group.getGroup();

                                sourcesByPartition.put(
                                        partition,
                                        Flux.from(group)
                                                .log("partition-" + partition, Level.WARNING, SignalType.ON_ERROR)
                                                .retry()
                                                .doFinally(__ -> sourcesByPartition.remove(partition))
                                );

                                return SubscribeReply.newBuilder()
                                        .setAssignment(Assignment.newBuilder()
                                                .setPartition(partition)
                                                .setSessionId(sessionId)
                                        )
                                        .build();
                            })
                            .doFinally(__ -> {
                                sources.remove(sessionId, sourcesByPartition);
                                subscriptions.remove(sessionId, subscription);
                            });
                })
                .log("subscribe", Level.SEVERE, SignalType.ON_ERROR);
    }

    @Override
    public Flux<ReceiveReply> receive(Mono<ReceiveRequest> requestMono) {
        return requestMono
                .flatMapMany(request -> {
                    String sessionId = request.getAssignment().getSessionId();
                    int partition = request.getAssignment().getPartition();
                    // TODO auto ack to the last known offset
                    long lastKnownOffset = request.getLastKnownOffset();

                    Flux<Record> source = sources.get(sessionId).get(partition);

                    if (source == null) {
                        log.warn("Source is null, returning empty Publisher. Request: {}", request.toString().replace("\n", "\\n"));
                        return Mono.empty();
                    }

                    for (RecordPostProcessor processor : recordPostProcessorChain.getAll()) {
                        source = source.transform(processor::postProcess);
                    }

                    return source
                            .map(consumerRecord -> ReceiveReply.newBuilder()
                                    .setRecord(
                                            ReceiveReply.Record.newBuilder()
                                                    .setOffset(consumerRecord.getOffset())
                                                    .setKey(ByteString.copyFrom(consumerRecord.getEnvelope().getKey()))
                                                    .setValue(ByteString.copyFrom(consumerRecord.getEnvelope().getValue()))
                                                    .setTimestamp(Timestamp.newBuilder()
                                                            .setSeconds(consumerRecord.getTimestamp().getEpochSecond())
                                                            .setNanos(consumerRecord.getTimestamp().getNano())
                                                    )
                                    )
                                    .build()
                            );
                })
                .log("receive", Level.SEVERE, SignalType.ON_ERROR);
    }

    @Override
    public Mono<Empty> ack(Mono<AckRequest> request) {
        return request
                .flatMap(ack -> {
                    StoredSubscription subscription = subscriptions.get(ack.getAssignment().getSessionId());

                    if (subscription == null) {
                        log.warn("Subscription is null, returning empty Publisher. Request: {}", ack.toString().replace("\n", "\\n"));
                        return Mono.empty();
                    }

                    return Mono.fromCompletionStage(positionsStorage.update(
                            subscription.getTopic(),
                            subscription.getGroupId(),
                            ack.getAssignment().getPartition(),
                            ack.getOffset()
                    ));
                })
                .then(Mono.just(Empty.getDefaultInstance()))
                .log("ack", Level.SEVERE, SignalType.ON_ERROR);
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(Mono<GetOffsetsRequest> request) {
        return request.flatMap(getOffsets -> Mono
                .fromCompletionStage(positionsStorage.findAll(getOffsets.getTopic(), getOffsets.getGroup()))
                .defaultIfEmpty(Collections.emptyMap())
                .map(offsets -> GetOffsetsReply.newBuilder().putAllOffsets(offsets).build())
        );
    }

    @Value
    private static class StoredSubscription {

        Subscription subscription;

        String topic;

        String groupId;
    }
}
