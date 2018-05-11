package com.github.bsideup.liiklus.records;

import lombok.Value;
import lombok.experimental.Delegate;
import lombok.experimental.Wither;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface RecordsStorage {

    CompletionStage<OffsetInfo> publish(Envelope envelope);

    Subscription subscribe(String topic, String groupId, Optional<String> autoOffsetReset);

    interface Subscription {

        Publisher<? extends GroupedPublisher<Integer, Record>> getPublisher();
    }

    @Value
    class OffsetInfo {

        String topic;

        int partition;

        long offset;
    }

    @Value
    @Wither
    class Envelope {

        String topic;

        ByteBuffer key;

        ByteBuffer value;
    }

    @Value
    class Record {

        Envelope envelope;

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