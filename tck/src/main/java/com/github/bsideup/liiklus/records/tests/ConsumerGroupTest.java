package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage.OffsetInfo;
import com.github.bsideup.liiklus.records.RecordsStorage.PartitionSource;
import com.github.bsideup.liiklus.records.RecordsStorage.Subscription;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public interface ConsumerGroupTest extends RecordStorageTestSupport {

    int getNumberOfPartitions();

    String keyByPartition(int partition);

    default Map<Integer, Long> publishToEveryPartition() {
        return IntStream.range(0, getNumberOfPartitions())
                .parallel()
                .mapToObj(partition -> publish(keyByPartition(partition).getBytes(), new byte[1]))
                .collect(Collectors.toMap(
                        OffsetInfo::getPartition,
                        OffsetInfo::getOffset
                ));
    }

    @Test
    default void testMultipleGroups() throws Exception {
        var numberOfPartitions = getNumberOfPartitions();
        Assumptions.assumeTrue(numberOfPartitions > 1, "target supports more than 1 partition");

        var groupName = UUID.randomUUID().toString();

        var receivedOffsets = new HashMap<Subscription, Map<Integer, Long>>();

        var disposeAll = ReplayProcessor.<Boolean>create(1);

        Function<Subscription, Disposable> subscribeAndAssign = subscription -> {
            return Flux.from(subscription.getPublisher(() -> CompletableFuture.completedFuture(Collections.emptyMap())))
                    .flatMap(Flux::fromStream, numberOfPartitions)
                    .flatMap(PartitionSource::getPublisher, numberOfPartitions)
                    .takeUntilOther(disposeAll)
                    .subscribe(record -> {
                        receivedOffsets
                                .computeIfAbsent(subscription, __ -> new HashMap<>())
                                .put(record.getPartition(), record.getOffset());
                    });
        };

        try {
            var firstSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("latest"));
            var secondSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("latest"));
            var lastOffsets = new HashMap<Integer, Long>();

            var firstDisposable = subscribeAndAssign.apply(firstSubscription);
            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets).hasEntrySatisfying(firstSubscription, it -> assertThat(it).isEqualTo(lastOffsets));
                    assertThat(receivedOffsets).doesNotContainKey(secondSubscription);
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
                    throw e;
                }
            });
            receivedOffsets.clear();

            var secondDisposable = subscribeAndAssign.apply(secondSubscription);
            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets).hasEntrySatisfying(firstSubscription, it -> assertThat(it).isNotEmpty());
                    assertThat(receivedOffsets).hasEntrySatisfying(secondSubscription, it -> assertThat(it).isNotEmpty());
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
                    throw e;
                }
            });
            receivedOffsets.clear();

            secondDisposable.dispose();
            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets).hasEntrySatisfying(firstSubscription, it -> assertThat(it).isEqualTo(lastOffsets));
                    assertThat(receivedOffsets).doesNotContainKey(secondSubscription);
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
                    throw e;
                }
            });
            receivedOffsets.clear();

            subscribeAndAssign.apply(secondSubscription);
            firstDisposable.dispose();
            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets).doesNotContainKey(firstSubscription);
                    assertThat(receivedOffsets).hasEntrySatisfying(secondSubscription, it -> assertThat(it).isEqualTo(lastOffsets));
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
                    throw e;
                }
            });
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    default void testExclusiveRecordDistribution() throws Exception {
        var numberOfPartitions = getNumberOfPartitions();
        Assumptions.assumeTrue(numberOfPartitions > 1, "target supports more than 1 partition");

        var groupName = UUID.randomUUID().toString();

        var receivedOffsets = new ConcurrentHashMap<Subscription, Set<Tuple2<Integer, Long>>>();

        var disposeAll = ReplayProcessor.<Boolean>create(1);

        Function<Subscription, Disposable> subscribeAndAssign = subscription -> {
            return Flux.from(subscription.getPublisher(() -> CompletableFuture.completedFuture(Collections.emptyMap())))
                    .flatMap(Flux::fromStream, numberOfPartitions)
                    .flatMap(PartitionSource::getPublisher, numberOfPartitions)
                    .takeUntilOther(disposeAll)
                    .subscribe(record -> {
                        receivedOffsets
                                .computeIfAbsent(subscription, __ -> new HashSet<>())
                                .add(Tuples.of(record.getPartition(), record.getOffset()));
                    });
        };

        try {
            var firstSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("earliest"));
            var secondSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("earliest"));

            subscribeAndAssign.apply(firstSubscription);
            subscribeAndAssign.apply(secondSubscription);

            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets)
                            .containsKeys(firstSubscription, secondSubscription)
                            .allSatisfy((key, value) -> assertThat(value).isNotEmpty());
                } catch (Throwable e) {
                    publishToEveryPartition();
                    throw e;
                }
            });

            assertThat(receivedOffsets.get(firstSubscription))
                    .doesNotContainAnyElementsOf(receivedOffsets.get(secondSubscription));
        } finally {
            disposeAll.onNext(true);
        }
    }


    @Test
    default void shouldAlwaysUseEarliestOffsetOnEmptyOffsetsInTheInitialProvider() {
        String groupName = "name";
        var earliest = "earliest";
        int count = 10;
        int offsetShift = 5;

        var published = publishMany(UUID.randomUUID().toString().getBytes(), count);
        var latest = published.get(published.size() - 1);

        RecordsStorage.Record latestRecord = subscribeToPartitionWithGroup(groupName, latest.getPartition(), earliest, () -> CompletableFuture.completedStage(Map.of(latest.getPartition(), latest.getOffset() - offsetShift)))
                .flatMap(PartitionSource::getPublisher)
                .takeUntil(next -> next.getOffset() == latest.getOffset())
                .blockLast(Duration.ofSeconds(10));

        assertThat(latestRecord.getOffset()).describedAs("latest offset").isEqualTo(latest.getOffset());

        List<RecordsStorage.Record> records = subscribeToPartitionWithGroup(groupName, latest.getPartition(), earliest, () -> CompletableFuture.completedStage(Map.of()))
                .flatMap(PartitionSource::getPublisher)
                .takeUntil(next -> next.getOffset() == latest.getOffset())
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(records).hasSize(count);
    }


    default Flux<? extends RecordsStorage.PartitionSource> subscribeToPartitionWithGroup(
            String groupName,
            int partition,
            String offsetReset,
            Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider
    ) {
        return Flux.from(getTarget().subscribe(getTopic(), groupName, Optional.of(offsetReset)).getPublisher(offsetsProvider))
                .flatMapIterable(it -> it::iterator)
                .filter(it -> partition == it.getPartition());
    }
}
