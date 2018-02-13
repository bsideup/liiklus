package com.github.bsideup.liiklus.source;

import lombok.Value;
import lombok.experimental.Delegate;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;

public interface KafkaSource {

    Subscription subscribe(Map<String, Object> props, String topic);

    interface Subscription {

        Publisher<? extends GroupedPublisher<Integer, KafkaRecord>> getPublisher();

        Mono<Void> acknowledge(int partition, long offset);
    }

    @Value
    class KafkaRecord {

        ByteBuffer key;

        ByteBuffer value;

        Instant timestamp;

        int partition;

        long offset;
    }

    interface GroupedPublisher<G, T> extends Publisher<T> {
        G getGroup();
    }

    @Value
    class DelegatingGroupedPublisher<G, T> implements GroupedPublisher<G, T> {

        G group;

        @Delegate(types = Publisher.class)
        Publisher<T> delegate;
    }
}