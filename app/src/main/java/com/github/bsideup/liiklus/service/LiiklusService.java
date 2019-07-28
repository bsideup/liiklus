package com.github.bsideup.liiklus.service;

import com.github.bsideup.liiklus.config.RecordPostProcessorChain;
import com.github.bsideup.liiklus.config.RecordPreProcessorChain;
import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.records.FiniteRecordsStorage;
import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;
import com.github.bsideup.liiklus.records.RecordsStorage.Record;
import com.github.bsideup.liiklus.records.RecordsStorage.Subscription;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
public class LiiklusService {

    private static final NavigableMap<Integer, Map<Integer, Long>> EMPTY_ACKED_OFFSETS = Collections.unmodifiableNavigableMap(new TreeMap<>());

    ConcurrentMap<String, StoredSubscription> subscriptions = new ConcurrentHashMap<>();

    ConcurrentMap<String, ConcurrentMap<Integer, StoredSource>> sources = new ConcurrentHashMap<>();

    RecordsStorage recordsStorage;

    PositionsStorage positionsStorage;

    RecordPreProcessorChain recordPreProcessorChain;

    RecordPostProcessorChain recordPostProcessorChain;

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
                                return Mono.fromCompletionStage(processor.preProcess(envelope))
                                        .onErrorMap(e -> new PreProcessorException(processor, e));
                            } catch (Throwable e) {
                                return Mono.error(new PreProcessorException(processor, e));
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

    public Flux<SubscribeReply> subscribe(Mono<SubscribeRequest> requestFlux) {
        return requestFlux
                .flatMapMany(subscribe -> {
                    var groupVersion = subscribe.getGroupVersion();
                    final GroupId groupId;
                    if (groupVersion != 0) {
                        groupId = GroupId.of(subscribe.getGroup(), groupVersion);
                    } else {
                        // Support legacy versioned groups
                        String group = subscribe.getGroup();
                        groupId = GroupId.ofString(group);

                        groupId.getVersion().ifPresent(it -> {
                            log.warn("Parsed a legacy group '{}' into {}", group, groupId);
                        });
                    }
                    var topic = subscribe.getTopic();

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

                    var sessionId = UUID.randomUUID().toString();

                    var storedSubscription = new StoredSubscription(topic, groupId);
                    subscriptions.put(sessionId, storedSubscription);

                    var sourcesByPartition = sources.computeIfAbsent(sessionId, __ -> new ConcurrentHashMap<>());

                    Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider = () -> {
                        return getOffsetsByGroupName(topic, groupId.getName())
                                .map(ackedOffsets -> {
                                    return groupId.getVersion()
                                            .map(version -> ackedOffsets.getOrDefault(version, emptyMap()))
                                            .orElse(ackedOffsets.isEmpty() ? emptyMap() : ackedOffsets.firstEntry().getValue());
                                })
                                .map(latestAckedOffsets -> latestAckedOffsets
                                        .entrySet()
                                        .stream()
                                        .collect(Collectors.toMap(it -> it.getKey(), it -> it.getValue() + 1))
                                )
                                .toFuture();
                    };

                    var subscription = recordsStorage.subscribe(topic, groupId.getName(), autoOffsetReset);

                    return Flux.from(subscription.getPublisher(offsetsProvider))
                            .flatMap(sources -> Flux.fromStream(sources).map(source -> {
                                var partition = source.getPartition();

                                sourcesByPartition.put(
                                        partition,
                                        new StoredSource(
                                                topic,
                                                groupId,
                                                Flux.from(source.getPublisher())
                                                        .log("partition-" + partition, Level.WARNING, SignalType.ON_ERROR)
                                                        .doFinally(__ -> sourcesByPartition.remove(partition))
                                                        // TODO .transform(Operators.lift(new SubscribeOnlyOnceLifter<Record>()))
                                        )
                                );

                                return SubscribeReply.newBuilder()
                                        .setAssignment(
                                                Assignment.newBuilder()
                                                        .setPartition(partition)
                                                        .setSessionId(sessionId)
                                        )
                                        .build();
                            }))
                            .doFinally(__ -> {
                                sources.remove(sessionId, sourcesByPartition);
                                subscriptions.remove(sessionId, storedSubscription);
                            });
                })
                .log("subscribe", Level.SEVERE, SignalType.ON_ERROR);
    }

    public Flux<ReceiveReply> receive(Mono<ReceiveRequest> requestMono) {
        return requestMono
                .flatMapMany(request -> {
                    String sessionId = request.getAssignment().getSessionId();
                    int partition = request.getAssignment().getPartition();
                    // TODO auto ack to the last known offset
                    long lastKnownOffset = request.getLastKnownOffset();

                    var storedSource = sources.containsKey(sessionId) ? sources.get(sessionId).get(partition) : null;

                    if (storedSource == null) {
                        log.warn("Source is null, returning empty Publisher. Request: {}", request.toString().replace("\n", "\\n"));
                        return Mono.empty();
                    }

                    return getLatestOffsetsOfGroup(storedSource.getTopic(), storedSource.getGroupId().getName()).flatMapMany(latestAckedOffsets -> {
                        Flux<Record> records = storedSource.getRecords();

                        for (RecordPostProcessor processor : recordPostProcessorChain.getAll()) {
                            records = records.transform(processor::postProcess);
                        }
                        return records
                                .map(consumerRecord -> {
                                    var envelope = consumerRecord.getEnvelope();

                                    var replyBuilder = ReceiveReply.Record.newBuilder()
                                            .setOffset(consumerRecord.getOffset())
                                            .setReplay(consumerRecord.getOffset() <= latestAckedOffsets.getOrDefault(partition, Optional.empty()).orElse(-1L))
                                            .setValue(ByteString.copyFrom(envelope.getValue()))
                                            .setTimestamp(Timestamp.newBuilder()
                                                    .setSeconds(consumerRecord.getTimestamp().getEpochSecond())
                                                    .setNanos(consumerRecord.getTimestamp().getNano())
                                            );

                                    if (envelope.getKey() != null) {
                                        replyBuilder.setKey(ByteString.copyFrom(envelope.getKey()));
                                    }

                                    return ReceiveReply.newBuilder().setRecord(replyBuilder).build();
                                });
                    });
                })
                .log("receive", Level.SEVERE, SignalType.ON_ERROR);
    }

    public Mono<Empty> ack(Mono<AckRequest> request) {
        return request
                .flatMap(ack -> {
                    String topic;
                    GroupId groupId;
                    int partition;

                    if (ack.hasAssignment()) {
                        var subscription = subscriptions.get(ack.getAssignment().getSessionId());

                        if (subscription == null) {
                            log.warn("Subscription is null, returning empty Publisher. Request: {}", ack.toString().replace("\n", "\\n"));
                            return Mono.empty();
                        }

                        topic = subscription.getTopic();
                        groupId = subscription.getGroupId();
                        partition = ack.getAssignment().getPartition();
                    } else {
                        topic = ack.getTopic();
                        groupId = GroupId.of(ack.getGroup(), ack.getGroupVersion());
                        partition = ack.getPartition();
                    }

                    return Mono.fromCompletionStage(positionsStorage.update(
                            topic,
                            groupId,
                            partition,
                            ack.getOffset()
                    ));
                })
                .thenReturn(Empty.getDefaultInstance())
                .log("ack", Level.SEVERE, SignalType.ON_ERROR);
    }

    public Mono<GetOffsetsReply> getOffsets(Mono<GetOffsetsRequest> request) {
        return request.flatMap(getOffsets -> Mono
                .fromCompletionStage(positionsStorage.findAll(
                        getOffsets.getTopic(),
                        GroupId.of(
                                getOffsets.getGroup(),
                                getOffsets.getGroupVersion()
                        )
                ))
                .defaultIfEmpty(emptyMap())
                .map(offsets -> GetOffsetsReply.newBuilder().putAllOffsets(offsets).build())
                .log("getOffsets", Level.SEVERE, SignalType.ON_ERROR)
        );
    }

    public Mono<GetEndOffsetsReply> getEndOffsets(Mono<GetEndOffsetsRequest> request) {
        return request.flatMap(getEndOffsets -> {
            if (!(recordsStorage instanceof FiniteRecordsStorage)) {
                return Mono.error(new IllegalStateException("The record storage is not finite"));
            }

            var topic = getEndOffsets.getTopic();
            return Mono.fromCompletionStage(((FiniteRecordsStorage) recordsStorage).getEndOffsets(topic))
                    .map(endOffsets -> GetEndOffsetsReply.newBuilder().putAllOffsets(endOffsets).build());
        });
    }

    private Mono<Map<Integer, Optional<Long>>> getLatestOffsetsOfGroup(String topic, String groupName) {
        return getOffsetsByGroupName(topic, groupName)
                .map(ackedOffsets -> ackedOffsets.values().stream()
                        .flatMap(it -> it.entrySet().stream())
                        .collect(Collectors.groupingBy(
                                Map.Entry::getKey,
                                Collectors.mapping(
                                        Map.Entry::getValue,
                                        Collectors.maxBy(Comparator.comparingLong(it -> it))
                                )
                        ))
                );
    }

    private Mono<NavigableMap<Integer, Map<Integer, Long>>> getOffsetsByGroupName(String topic, String groupName) {
        return Mono
                .fromCompletionStage(positionsStorage.findAllVersionsByGroup(topic, groupName))
                .<NavigableMap<Integer, Map<Integer, Long>>>map(TreeMap::new)
                .defaultIfEmpty(EMPTY_ACKED_OFFSETS);
    }

    @Value
    private static class StoredSubscription {

        String topic;

        GroupId groupId;
    }

    @Value
    private static class StoredSource {

        String topic;

        GroupId groupId;

        Flux<Record> records;
    }

    private static class PreProcessorException extends RuntimeException {

        public PreProcessorException(@NonNull RecordPreProcessor preProcessor, Throwable cause) {
            super(preProcessor.getClass().getName() + ": " + cause.getMessage(), cause);
        }
    }
}
