package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage.PartitionSource;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.apache.pulsar.client.util.MathUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.PulsarContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class PulsarRecordsStorageTest implements RecordStorageTests {

    private static final int NUM_OF_PARTITIONS = 4;

    // Generate a set of keys where each key goes to unique partition
    public static Map<Integer, String> PARTITION_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .scanWith(
                    () -> new HashMap<Integer, String>(),
                    (acc, it) -> {
                        acc.put(MathUtils.signSafeMod(Murmur3_32Hash.getInstance().makeHash(it), NUM_OF_PARTITIONS), it);
                        return acc;
                    }
            )
            .filter(it -> it.size() == NUM_OF_PARTITIONS)
            .blockFirst(Duration.ofSeconds(10));

    private static final PulsarContainer pulsar = new PulsarContainer();

    static final ApplicationContext applicationContext;

    static {
        pulsar.start();

        applicationContext = new ApplicationRunner("PULSAR", "MEMORY")
                .withProperty("pulsar.serviceUrl", pulsar.getPulsarBrokerUrl())
                .run();
    }

    @Getter
    RecordsStorage target = applicationContext.getBean(RecordsStorage.class);

    @Getter
    String topic = UUID.randomUUID().toString();

    @SneakyThrows
    public PulsarRecordsStorageTest() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();

        pulsarAdmin.topics().createPartitionedTopic(topic, getNumberOfPartitions());
    }

    @Override
    public String keyByPartition(int partition) {
        return PARTITION_KEYS.get(partition);
    }

    @Override
    public int getNumberOfPartitions() {
        return NUM_OF_PARTITIONS;
    }

    @Test
    @Override
    // since pulsar behave differently for closed ledger, need to overwrite this test to include the 1 entry setback
    public void testInitialOffsets() throws Exception {
        var offsetInfos = publishMany("key".getBytes(), 10);
        var offsetInfo = offsetInfos.get(7);
        var partition = offsetInfo.getPartition();
        var position = offsetInfo.getOffset();

        var receivedRecords = subscribeToPartition(partition, Optional.of("earliest"), () -> CompletableFuture.completedFuture(Collections.singletonMap(partition, position)))
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .take(4)
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(receivedRecords).extracting(RecordsStorage.Record::getOffset).containsExactly(
                offsetInfos.get(6).getOffset(),
                offsetInfos.get(7).getOffset(),
                offsetInfos.get(8).getOffset(),
                offsetInfos.get(9).getOffset()
        );
    }

    @Test
    @Override
    // currently each liiklus consumers consume all partition, and leaves the client to rebalance
    // in pulsar, the pulsar client does not rebalance naturally on failover or exclusive
    public void testExclusiveRecordDistribution() throws Exception {
        var numberOfPartitions = getNumberOfPartitions();
        Assumptions.assumeTrue(numberOfPartitions > 1, "target supports more than 1 partition");

        var groupName = UUID.randomUUID().toString();

        var receivedOffsets = new ConcurrentHashMap<RecordsStorage.Subscription, Set<Tuple2<Integer, Long>>>();

        var disposeAll = ReplayProcessor.<Boolean>create(1);

        Function<RecordsStorage.Subscription, Disposable> subscribeAndAssign = subscription -> {
            return Flux.from(subscription.getPublisher(() -> CompletableFuture.completedFuture(Collections.emptyMap())))
                    .flatMap(Flux::fromStream, numberOfPartitions)
                    .flatMap(RecordsStorage.PartitionSource::getPublisher, numberOfPartitions)
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
            await.untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets)
                            .containsKeys(firstSubscription)
                            .doesNotContainKey(secondSubscription)
                            .allSatisfy((key, value) -> assertThat(value).isNotEmpty());
                } catch (Throwable e) {
                    publishToEveryPartition();
                    throw e;
                }
            });

            subscribeAndAssign.apply(secondSubscription);
            publishToEveryPartition();

            await.pollDelay(org.awaitility.Duration.TWO_SECONDS).untilAsserted(() -> {
                try {
                    assertThat(receivedOffsets)
                            .containsKeys(firstSubscription)
                            .doesNotContainKey(secondSubscription)
                            .allSatisfy((key, value) -> assertThat(value).isNotEmpty());
                } catch (Throwable e) {
                    publishToEveryPartition();
                    throw e;
                }
            });
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    @Override
    // currently each liiklus consumers consume all partition, and leaves the client to rebalance
    // in pulsar, the pulsar client does not rebalance naturally on failover or exclusive
    public void testMultipleGroups() throws Exception {
        var numberOfPartitions = getNumberOfPartitions();
        Assumptions.assumeTrue(numberOfPartitions > 1, "target supports more than 1 partition");

        var groupName = UUID.randomUUID().toString();

        var receivedOffsets = new HashMap<RecordsStorage.Subscription, Map<Integer, Long>>();

        var disposeAll = ReplayProcessor.<Boolean>create(1);

        Function<RecordsStorage.Subscription, Disposable> subscribeAndAssign = subscription -> {
            return Flux.from(subscription.getPublisher(() -> CompletableFuture.completedFuture(Collections.emptyMap())))
                    .flatMap(Flux::fromStream, numberOfPartitions)
                    .flatMap(RecordsStorage.PartitionSource::getPublisher, numberOfPartitions)
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
                    assertThat(receivedOffsets).hasEntrySatisfying(firstSubscription, it -> assertThat(it).isEqualTo(lastOffsets));
                    assertThat(receivedOffsets).doesNotContainKey(secondSubscription);
                } catch (Throwable e) {
                    lastOffsets.putAll(publishToEveryPartition());
                    throw e;
                }
            });
            receivedOffsets.clear();

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
            receivedOffsets.clear();

            subscribeAndAssign.apply(firstSubscription);
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
        } finally {
            disposeAll.onNext(true);
        }
    }

    @Test
    void shouldPreferEventTimeOverPublishTime() throws Exception {
        var topic = getTopic();
        var partition = 0;
        var key = keyByPartition(partition);
        var eventTimestamp = Instant.now().minusSeconds(1000).truncatedTo(ChronoUnit.MILLIS);

        try (
                var pulsarClient = PulsarClient.builder()
                        .serviceUrl(pulsar.getPulsarBrokerUrl())
                        .build()
        ) {
            pulsarClient.newProducer()
                    .topic(topic)
                    .hashingScheme(HashingScheme.Murmur3_32Hash)
                    .create()
                    .newMessage()
                    .key(key)
                    .value("hello".getBytes())
                    .eventTime(eventTimestamp.toEpochMilli())
                    .send();
        }

        var record = subscribeToPartition(partition)
                .flatMap(PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(record).satisfies(it -> {
            assertThat(it.getTimestamp()).isEqualTo(eventTimestamp);
        });
    }
}