package com.github.bsideup.liiklus.records.inmemory;

import com.github.bsideup.liiklus.records.FiniteRecordsStorage;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * WARNING: this storage type should only be used for testing and NOT in production
 */
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class InMemoryRecordsStorage implements FiniteRecordsStorage {

    public static int partitionByKey(String key, int numberOfPartitions) {
        return partitionByKey(ByteBuffer.wrap(key.getBytes()), numberOfPartitions);
    }

    public static int partitionByKey(ByteBuffer key, int numberOfPartitions) {
        return Math.abs(key.hashCode()) % numberOfPartitions;
    }

    int numberOfPartitions;

    ConcurrentMap<String, StoredTopic> state = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<OffsetInfo> publish(Envelope envelope) {
        var topic = envelope.getTopic();
        var storedTopic = state.computeIfAbsent(topic, __ -> new StoredTopic(numberOfPartitions));

        var partition = envelope.getKey() != null
                ? partitionByKey(envelope.getKey(), numberOfPartitions)
                : ThreadLocalRandom.current().nextInt(0, numberOfPartitions);
        var storedPartition = storedTopic.getPartitions().computeIfAbsent(
                partition,
                __ -> new StoredTopic.StoredPartition()
        );

        var offset = storedPartition.getNextOffset().getAndIncrement();
        storedPartition.getProcessor().onNext(new StoredTopic.StoredPartition.StoredRecord(
                offset,
                envelope.getKey(),
                envelope.getValue()
        ));

        return CompletableFuture.completedFuture(new OffsetInfo(
                topic,
                partition,
                offset
        ));
    }

    @Override
    public CompletionStage<Map<Integer, Long>> getEndOffsets(String topic) {
        var partitions = state.getOrDefault(topic, new StoredTopic(numberOfPartitions)).getPartitions();
        return CompletableFuture.completedFuture(
                partitions.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        it -> Math.max(
                                0,
                                it.getValue().getNextOffset().get() - 1
                        )
                ))
        );
    }

    @Override
    public Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset) {
        var storedTopic = state.computeIfAbsent(topic, __ -> new StoredTopic(numberOfPartitions));
        return new Subscription() {

            @Override
            public Publisher<Stream<? extends PartitionSource>> getPublisher(
                    Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider
            ) {
                var subscription = this;
                return Flux.create(sink -> {

                    sink.onCancel(() -> storedTopic.revoke(groupName, this));

                    if (sink.isCancelled()) {
                        return;
                    }

                    storedTopic.assign(groupName, subscription);

                    sink.next(IntStream.range(0, numberOfPartitions).mapToObj(partition -> new PartitionSource() {

                        @Override
                        public int getPartition() {
                            return partition;
                        }

                        @Override
                        public Publisher<Record> getPublisher() {
                            var storedPartition = storedTopic.getPartitions().computeIfAbsent(
                                    partition,
                                    __ -> new StoredTopic.StoredPartition()
                            );
                            return Mono.defer(() -> Mono.fromCompletionStage(offsetsProvider.get()))
                                    .defaultIfEmpty(Collections.emptyMap())
                                    .flatMapMany(offsets -> storedPartition.getProcessor()
                                            .compose(flux -> {
                                                if (offsets.containsKey(partition)) {
                                                    return flux.skip(offsets.get(partition));
                                                }
                                                switch (autoOffsetReset.orElse("")) {
                                                    case "latest":
                                                        long nextOffset = storedPartition.getNextOffset().get();
                                                        if (nextOffset > 0) {
                                                            return flux.skip(nextOffset);
                                                        }
                                                    default:
                                                        return flux;
                                                }
                                            })
                                    )
                                    .filter(it -> storedTopic.isAssigned(groupName, subscription, partition))
                                    .map(it -> new Record(
                                            new Envelope(
                                                    topic,
                                                    it.getKey() != null ? it.getKey().asReadOnlyBuffer() : null,
                                                    it.getValue().asReadOnlyBuffer()
                                            ),
                                            it.getTimestamp(),
                                            partition,
                                            it.getOffset()
                                    ));
                        }
                    }));
                });
            }
        };
    }

    @Value
    static class StoredTopic {

        int numberOfPartitions;

        ConcurrentMap<Integer, StoredPartition> partitions = new ConcurrentHashMap<>();

        ConcurrentMap<String, ConcurrentMap<Subscription, Set<Integer>>> groupAssignments = new ConcurrentHashMap<>();

        synchronized void assign(String groupName, Subscription subscription) {
            var groupAssignment = groupAssignments.computeIfAbsent(groupName, ___ -> new ConcurrentHashMap<>());
            groupAssignment.put(subscription, Collections.emptySet());
            rebalance(groupName);
        }

        synchronized void revoke(String groupName, Subscription subscription) {
            var groupAssignment = groupAssignments.computeIfAbsent(groupName, ___ -> new ConcurrentHashMap<>());
            groupAssignment.remove(subscription);
            rebalance(groupName);
        }

        synchronized boolean isAssigned(String groupName, Subscription subscription, int partition) {
            return groupAssignments.get(groupName).get(subscription).contains(partition);
        }

        private synchronized void rebalance(String groupName) {
            var subscriptions = groupAssignments.get(groupName);

            var i = new AtomicLong();
            int subscriptionsNum = subscriptions.size();
            for (var subscription : subscriptions.keySet()) {
                var entryNum = i.getAndIncrement();
                subscriptions.put(
                        subscription,
                        IntStream
                                .range(0, numberOfPartitions)
                                .filter(it -> it % subscriptionsNum == entryNum)
                                .boxed()
                                .collect(Collectors.toSet())
                );
            }
        }

        @Value
        static class StoredPartition {

            AtomicLong nextOffset = new AtomicLong(0);

            FluxProcessor<StoredRecord, StoredRecord> processor = ReplayProcessor.create(Integer.MAX_VALUE);

            @Value
            static class StoredRecord {

                Instant timestamp = Instant.now();

                long offset;

                ByteBuffer key;

                ByteBuffer value;
            }
        }
    }
}
