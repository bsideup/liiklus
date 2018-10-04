package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage.OffsetInfo;
import com.github.bsideup.liiklus.records.RecordsStorage.PartitionSource;
import com.github.bsideup.liiklus.records.RecordsStorage.Subscription;
import lombok.val;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
        val numberOfPartitions = getNumberOfPartitions();
        Assumptions.assumeTrue(numberOfPartitions > 1, "target supports more than 1 partition");

        val groupName = UUID.randomUUID().toString();

        val receivedOffsets = new HashMap<Subscription, Map<Integer, Long>>();

        val disposeAll = ReplayProcessor.<Boolean>create(1);

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
            val firstSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("latest"));
            val secondSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("latest"));

            val firstDisposable = subscribeAndAssign.apply(firstSubscription);

            val lastOffsets = new HashMap<Integer, Long>();

            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets).hasEntrySatisfying(firstSubscription, it -> assertThat(it).isEqualTo(lastOffsets));
                    assertThat(receivedOffsets).doesNotContainKey(secondSubscription);
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
                }
            });
            receivedOffsets.clear();

            val secondDisposable = subscribeAndAssign.apply(secondSubscription);

            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets).hasEntrySatisfying(firstSubscription, it -> assertThat(it).isNotEmpty());
                    assertThat(receivedOffsets).hasEntrySatisfying(secondSubscription, it -> assertThat(it).isNotEmpty());
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
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
                }
            });
        } finally {
            disposeAll.onNext(true);
        }
    }
}
