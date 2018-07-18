package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.val;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public interface ConsumerGroupTest extends RecordStorageTestSupport {

    int getNumberOfPartitions();

    @Test
    default void testMultipleGroups() throws Exception {
        val numberOfPartitions = getNumberOfPartitions();
        assertThat(numberOfPartitions).as("number of partitions").isGreaterThanOrEqualTo(2);

        val groupName = UUID.randomUUID().toString();

        val assignments = new HashMap<Integer, RecordsStorage.Subscription>();

        val disposeAll = ReplayProcessor.<Boolean>create(1);

        Function<RecordsStorage.Subscription, Disposable> subscribeAndAssign = subscription -> {
            return Flux.from(subscription.getPublisher())
                    .flatMap(Flux::fromStream)
                    .takeUntilOther(disposeAll)
                    .subscribe(source -> assignments.put(source.getPartition(), subscription));
        };

        try {
            val firstSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("earliest"));
            val secondSubscription = getTarget().subscribe(getTopic(), groupName, Optional.of("earliest"));

            val firstDisposable = subscribeAndAssign.apply(firstSubscription);
            await.untilAsserted(() -> {
                assertThat(assignments).hasSize(numberOfPartitions);
                assertThat(assignments).containsValue(firstSubscription);
                assertThat(assignments).doesNotContainValue(secondSubscription);
            });

            val secondDisposable = subscribeAndAssign.apply(secondSubscription);
            await.untilAsserted(() -> {
                assertThat(assignments).containsValues(firstSubscription, secondSubscription);
            });

            secondDisposable.dispose();
            await.untilAsserted(() -> {
                assertThat(assignments).doesNotContainValue(secondSubscription);
            });

            subscribeAndAssign.apply(secondSubscription);
            firstDisposable.dispose();
            await.untilAsserted(() -> {
                assertThat(assignments).containsValue(secondSubscription);
                assertThat(assignments).doesNotContainValue(firstSubscription);
            });
        } finally {
            disposeAll.onNext(true);
        }
    }
}
