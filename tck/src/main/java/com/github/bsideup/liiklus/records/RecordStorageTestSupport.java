package com.github.bsideup.liiklus.records;

import lombok.SneakyThrows;
import org.awaitility.core.ConditionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;

public interface RecordStorageTestSupport {

    static ConditionFactory await = await().atMost(30, TimeUnit.SECONDS);

    RecordsStorage getTarget();

    String getTopic();

    default RecordsStorage.Envelope createEnvelope(byte[] key) {
        return createEnvelope(key, UUID.randomUUID().toString().getBytes());
    }

    default RecordsStorage.Envelope createEnvelope(byte[] key, byte[] value) {
        return new RecordsStorage.Envelope(
                getTopic(),
                ByteBuffer.wrap(key),
                ByteBuffer.wrap(value)
        );
    }

    default List<RecordsStorage.OffsetInfo> publishMany(byte[] key, int num) {
        return publishMany(key, Flux.range(0, num).map(__ -> UUID.randomUUID().toString().getBytes()));
    }

    default List<RecordsStorage.OffsetInfo> publishMany(byte[] key, Flux<byte[]> values) {
        return values
                .flatMapSequential(value -> Mono.fromCompletionStage(getTarget().publish(createEnvelope(key, value))))
                .collectList()
                .block(Duration.ofSeconds(10));
    }

    default RecordsStorage.OffsetInfo publish(byte[] key, byte[] value) {
        return publish(createEnvelope(key, value));
    }

    @SneakyThrows
    default RecordsStorage.OffsetInfo publish(RecordsStorage.Envelope envelope) {
        return getTarget().publish(envelope).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    default Flux<? extends RecordsStorage.PartitionSource> subscribeToPartition(int partition) {
        return subscribeToPartition(partition, "earliest");
    }

    default Flux<? extends RecordsStorage.PartitionSource> subscribeToPartition(int partition, String offsetReset) {
        return subscribeToPartition(partition, Optional.of(offsetReset));
    }

    default Flux<? extends RecordsStorage.PartitionSource> subscribeToPartition(int partition, Optional<String> offsetReset) {
        return subscribeToPartition(partition, offsetReset, () -> CompletableFuture.completedFuture(Collections.emptyMap()));
    }

    default Flux<? extends RecordsStorage.PartitionSource> subscribeToPartition(
            int partition,
            Optional<String> offsetReset,
            Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider
    ) {
        return Flux.from(getTarget().subscribe(getTopic(), UUID.randomUUID().toString(), offsetReset).getPublisher(offsetsProvider))
                .flatMapIterable(it -> it::iterator)
                .filter(it -> partition == it.getPartition());
    }
}
