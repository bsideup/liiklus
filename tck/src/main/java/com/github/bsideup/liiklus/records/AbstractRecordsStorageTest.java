package com.github.bsideup.liiklus.records;

import com.github.bsideup.liiklus.records.RecordsStorage.*;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.awaitility.core.ConditionFactory;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RequiredArgsConstructor
public abstract class AbstractRecordsStorageTest<T extends RecordsStorage> {

    final T target;

    final String topic = UUID.randomUUID().toString();

    final ConditionFactory await = await().atMost(10, TimeUnit.SECONDS);

    abstract protected int getNumberOfPartitions();

    @Test
    public void testPublish() throws Exception {
        val record = createEnvelope("key".getBytes());

        val offsetInfo = publish(record);

        assertThat(offsetInfo)
                .satisfies(info -> {
                    assertThat(info.getTopic()).as("topic").isEqualTo(topic);
                    assertThat(info.getPartition()).as("partition").isNotNegative();
                    assertThat(info.getOffset()).as("offset").isNotNegative();
                });

        val receivedRecord = subscribeToPartition(offsetInfo.getPartition())
                .flatMap(PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(receivedRecord.getEnvelope()).as("envelope").isEqualTo(record);
        assertThat(receivedRecord.getPartition()).as("partition").isEqualTo(offsetInfo.getPartition());
        assertThat(receivedRecord.getOffset()).as("offset").isEqualTo(offsetInfo.getOffset());
    }

    @Test
    public void testPublishMany() {
        val numRecords = 5;

        val offsetInfos = publishMany("key".getBytes(), numRecords);

        assertThat(offsetInfos).hasSize(numRecords);

        val partition = offsetInfos.get(0).getPartition();

        assertThat(offsetInfos).extracting(OffsetInfo::getPartition).containsOnly(partition);
    }

    @Test
    public void testSubscribeWithEarliest() throws Exception {
        val numRecords = 5;
        val key = UUID.randomUUID().toString().getBytes();

        val offsetInfos = publishMany(key, numRecords);

        val partition = offsetInfos.get(0).getPartition();

        val disposeAll = DirectProcessor.<Boolean>create();

        try {
            val recordsSoFar = new ArrayList<Record>();

            subscribeToPartition(partition, "earliest")
                    .flatMap(PartitionSource::getPublisher)
                    .takeUntilOther(disposeAll)
                    .subscribe(recordsSoFar::add);

            await.untilAsserted(() -> {
                assertThat(recordsSoFar).hasSize(numRecords);
            });

            publish(key, UUID.randomUUID().toString().getBytes());

            await.untilAsserted(() -> {
                assertThat(recordsSoFar).hasSize(numRecords + 1);
            });
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    public void testSubscribeWithLatest() throws Exception {
        val key = UUID.randomUUID().toString().getBytes();

        val offsetInfos = publishMany(key, 5);

        val partition = offsetInfos.get(0).getPartition();

        val disposeAll = DirectProcessor.<Boolean>create();

        try {
            val recordsSoFar = new ArrayList<Record>();
            val assigned = new AtomicBoolean(false);

            subscribeToPartition(partition, "latest")
                    .doOnNext(__ -> assigned.set(true))
                    .flatMap(PartitionSource::getPublisher)
                    .takeUntilOther(disposeAll)
                    .subscribe(recordsSoFar::add);

            await.untilTrue(assigned);

            val envelope = createEnvelope(key);
            val offsetInfo = publish(envelope);

            await.untilAsserted(() -> {
                assertThat(recordsSoFar)
                        .hasSize(1)
                        .allSatisfy(it -> {
                            assertThat(it.getEnvelope()).as("envelope").isEqualTo(envelope);
                            assertThat(it.getPartition()).as("partition").isEqualTo(offsetInfo.getPartition());
                            assertThat(it.getOffset()).as("offset").isEqualTo(offsetInfo.getOffset());
                        });
            });
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    public void testSubscribeSorting() {
        val numRecords = 5;

        val offsetInfos = publishMany("key".getBytes(), numRecords);
        val partition = offsetInfos.get(0).getPartition();

        val records = subscribeToPartition(partition, "earliest")
                .flatMap(PartitionSource::getPublisher)
                .take(numRecords)
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(records)
                .isSortedAccordingTo(Comparator.comparingLong(Record::getOffset));
    }

    @Test
    public void testSubscribeBackpressure() throws Exception {
        val numRecords = 20;
        val key = "key".getBytes();

        val offsetInfos = publishMany(key, numRecords);
        val partition = offsetInfos.get(0).getPartition();

        val recordFlux = subscribeToPartition(partition, "earliest").flatMap(PartitionSource::getPublisher);

        val initialRequest = 1;
        StepVerifier.create(recordFlux, initialRequest)
                .expectSubscription()
                .expectNextCount(1)
                .then(() -> publishMany(key, 10))
                .thenRequest(1)
                .expectNextCount(1)
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    public void testSeekTo() throws Exception {
        val offsetInfos = publishMany("key".getBytes(), 10);
        val partition = offsetInfos.get(0).getPartition();

        val position = 7L;
        val receivedRecords = subscribeToPartition(partition, "earliest")
                .delayUntil(it -> Mono.fromCompletionStage(it.seekTo(position)))
                .flatMap(PartitionSource::getPublisher)
                .take(3)
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(receivedRecords).extracting(Record::getOffset).containsExactly(
                position + 0,
                position + 1,
                position + 2
        );
    }

    @Test
    public void testMultipleGroups() throws Exception {
        val numberOfPartitions = getNumberOfPartitions();
        assertThat(numberOfPartitions).as("number of partitions").isGreaterThanOrEqualTo(2);

        val groupName = UUID.randomUUID().toString();

        val assignments = new HashMap<Integer, Subscription>();

        val disposeAll = ReplayProcessor.<Boolean>create(1);

        Function<Subscription, Disposable> subscribeAndAssign = subscription -> {
            return Flux.from(subscription.getPublisher())
                    .flatMap(Flux::fromStream)
                    .takeUntilOther(disposeAll)
                    .subscribe(source -> assignments.put(source.getPartition(), subscription));
        };

        try {
            val firstSubscription = target.subscribe(topic, groupName, Optional.of("earliest"));
            val secondSubscription = target.subscribe(topic, groupName, Optional.of("earliest"));

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

    protected Envelope createEnvelope(byte[] key) {
        return createEnvelope(key, UUID.randomUUID().toString().getBytes());
    }

    protected Envelope createEnvelope(byte[] key, byte[] value) {
        return new Envelope(
                topic,
                ByteBuffer.wrap(key),
                ByteBuffer.wrap(value)
        );
    }

    protected List<OffsetInfo> publishMany(byte[] key, int num) {
        return publishMany(key, Flux.range(0, num).map(__ -> UUID.randomUUID().toString().getBytes()));
    }

    protected List<OffsetInfo> publishMany(byte[] key, Flux<byte[]> values) {
        return values
                .flatMapSequential(value -> Mono.fromCompletionStage(target.publish(createEnvelope(key, value))))
                .collectList()
                .block(Duration.ofSeconds(10));
    }

    protected OffsetInfo publish(byte[] key, byte[] value) {
        return publish(createEnvelope(key, value));
    }

    @SneakyThrows
    protected OffsetInfo publish(Envelope envelope) {
        return target.publish(envelope).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    protected Flux<? extends PartitionSource> subscribeToPartition(int partition) {
        return subscribeToPartition(partition, "earliest");
    }

    protected Flux<? extends PartitionSource> subscribeToPartition(int partition, String offsetReset) {
        return subscribeToPartition(partition, Optional.of(offsetReset));
    }

    protected Flux<? extends PartitionSource> subscribeToPartition(int partition, Optional<String> offsetReset) {
        return Flux.from(target.subscribe(topic, UUID.randomUUID().toString(), offsetReset).getPublisher())
                .flatMapIterable(it -> it::iterator)
                .filter(it -> partition == it.getPartition());
    }
}
