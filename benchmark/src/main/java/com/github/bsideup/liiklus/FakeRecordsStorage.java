package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@AutoService(ApplicationContextInitializer.class)
public class FakeRecordsStorage implements RecordsStorage, ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        if (!"FAKE".equals(applicationContext.getEnvironment().getProperty("storage.records.type"))) {
            return;
        }
        applicationContext.registerBean(FakeRecordsStorage.class);
    }

    @Override
    public CompletionStage<OffsetInfo> publish(Envelope envelope) {
        return CompletableFuture.failedStage(new IllegalStateException("unsupported"));
    }

    @Override
    public Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset) {
        var timestamp = Instant.now();

        AtomicLong counter = new AtomicLong();

        return offsetsProvider -> Flux.create(sink -> {
            sink.next(
                    IntStream.range(0, 32).mapToObj(partition -> new PartitionSource() {

                        AtomicLong offset = new AtomicLong();

                        @Override
                        public int getPartition() {
                            return partition;
                        }

                        @Override
                        public Publisher<Record> getPublisher() {
                            return Flux.<Record>generate(sink -> {
                                var envelope = new Envelope(
                                        topic,
                                        null,
                                        ByteBuffer.wrap(new UUID(counter.get(), 0).toString().getBytes())
                                );
                                sink.next(new Record(
                                        envelope,
                                        timestamp,
                                        partition,
                                        offset.getAndIncrement()
                                ));
                            }).subscribeOn(Schedulers.parallel());
                        }
                    })
            );
        });
    }
}
