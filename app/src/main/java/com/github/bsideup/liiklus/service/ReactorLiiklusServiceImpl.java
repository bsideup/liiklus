package com.github.bsideup.liiklus.service;

import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.RecordPostProcessorChain;
import com.github.bsideup.liiklus.config.RecordPreProcessorChain;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.GroupId;
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
import lombok.val;
import org.lognet.springboot.grpc.GRpcService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
@GRpcService
@GatewayProfile
public class ReactorLiiklusServiceImpl extends ReactorLiiklusServiceGrpc.LiiklusServiceImplBase {

    private static final NavigableMap<Integer, Map<Integer, Long>> EMPTY_ACKED_OFFSETS = Collections.unmodifiableNavigableMap(new TreeMap<>());

    ConcurrentMap<String, StoredSubscription> subscriptions = new ConcurrentHashMap<>();

    ConcurrentMap<String, ConcurrentMap<Integer, StoredSource>> sources = new ConcurrentHashMap<>();

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
                    val groupId = GroupId.of(subscribe.getGroup(), subscribe.getGroupVersion());
                    val topic = subscribe.getTopic();

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

                    val subscription = recordsStorage.subscribe(topic, groupId.getName(), autoOffsetReset);

                    val sessionId = UUID.randomUUID().toString();

                    val storedSubscription = new StoredSubscription(subscription, topic, groupId);
                    subscriptions.put(sessionId, storedSubscription);

                    val sourcesByPartition = sources.computeIfAbsent(sessionId, __ -> new ConcurrentHashMap<>());

                    return Flux.from(subscription.getPublisher())
                            .flatMap(sources -> Mono
                                    .fromCompletionStage(positionsStorage.findAllVersionsByGroup(topic, groupId.getName()))
                                    .<NavigableMap<Integer, Map<Integer, Long>>>map(TreeMap::new)
                                    .defaultIfEmpty(EMPTY_ACKED_OFFSETS)
                                    .flatMapMany(ackedOffsets -> Flux.fromStream(sources.map(source -> {
                                        val partition = source.getPartition();

                                        val latestAckedOffsets = ackedOffsets.values().stream()
                                                .flatMap(it -> it.entrySet().stream())
                                                .collect(Collectors.groupingBy(
                                                        Map.Entry::getKey,
                                                        Collectors.mapping(
                                                                Map.Entry::getValue,
                                                                Collectors.maxBy(Comparator.comparingLong(it -> it))
                                                        )
                                                ));

                                        sourcesByPartition.put(
                                                partition,
                                                new StoredSource(
                                                        latestAckedOffsets,
                                                        Mono
                                                                .defer(() -> {
                                                                    val offsets = groupId.getVersion()
                                                                            .map(version -> ackedOffsets.getOrDefault(version, emptyMap()))
                                                                            .orElse(ackedOffsets.isEmpty() ? emptyMap() : ackedOffsets.firstEntry().getValue());

                                                                    val lastAckedOffset = offsets.get(partition);
                                                                    if (lastAckedOffset != null) {
                                                                        return Mono.fromCompletionStage(source.seekTo(lastAckedOffset));
                                                                    } else {
                                                                        return Mono.empty();
                                                                    }
                                                                })
                                                                .cache()
                                                                .thenMany(Flux.from(source.getPublisher()))
                                                                .log("partition-" + partition, Level.WARNING, SignalType.ON_ERROR)
                                                                .doFinally(__ -> sourcesByPartition.remove(partition))
                                                )
                                        );

                                        return SubscribeReply.newBuilder()
                                                .setAssignment(
                                                        Assignment.newBuilder()
                                                                .setPartition(partition)
                                                                .setSessionId(sessionId)
                                                )
                                                .build();
                                    })))
                            )
                            .doFinally(__ -> {
                                sources.remove(sessionId, sourcesByPartition);
                                subscriptions.remove(sessionId, storedSubscription);
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

                    val storedSource = sources.get(sessionId).get(partition);
                    Flux<Record> records = storedSource.getRecords();

                    if (records == null) {
                        log.warn("Source is null, returning empty Publisher. Request: {}", request.toString().replace("\n", "\\n"));
                        return Mono.empty();
                    }

                    for (RecordPostProcessor processor : recordPostProcessorChain.getAll()) {
                        records = records.transform(processor::postProcess);
                    }

                    Long lastSeenOffset = storedSource.getLatestAckedOffsets().getOrDefault(partition, Optional.empty()).orElse(-1L);

                    return records
                            .map(consumerRecord -> ReceiveReply.newBuilder()
                                    .setRecord(
                                            ReceiveReply.Record.newBuilder()
                                                    .setOffset(consumerRecord.getOffset())
                                                    .setReplay(consumerRecord.getOffset() <= lastSeenOffset)
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
                    val subscription = subscriptions.get(ack.getAssignment().getSessionId());

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
                .thenReturn(Empty.getDefaultInstance())
                .log("ack", Level.SEVERE, SignalType.ON_ERROR);
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(Mono<GetOffsetsRequest> request) {
        return request.flatMap(getOffsets -> Mono
                .fromCompletionStage(positionsStorage.findAll(
                        getOffsets.getTopic(),
                        GroupId.of(
                                getOffsets.getGroup(),
                                getOffsets.getGroupVersion()
                        )
                ))
                .defaultIfEmpty(Collections.emptyMap())
                .map(offsets -> GetOffsetsReply.newBuilder().putAllOffsets(offsets).build())
        );
    }

    @Value
    private static class StoredSubscription {

        Subscription subscription;

        String topic;

        GroupId groupId;
    }

    @Value
    private static class StoredSource {

        Map<Integer, Optional<Long>> latestAckedOffsets;

        Flux<Record> records;
    }
}
