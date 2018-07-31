package com.github.bsideup.liiklus.records;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Wither;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

public interface RecordsStorage {

    CompletionStage<OffsetInfo> publish(Envelope envelope);

    Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset);

    interface Subscription {

        Publisher<Stream<? extends PartitionSource>> getPublisher();
    }

    @Value
    class OffsetInfo {

        String topic;

        int partition;

        long offset;
    }

    @Value
    @RequiredArgsConstructor
    class Envelope {

        String topic;

        Object rawKey;

        Function<Object, ByteBuffer> keyEncoder;

        @Getter(lazy = true)
        ByteBuffer key = keyEncoder.apply(rawKey);

        Object rawValue;

        Function<Object, ByteBuffer> valueEncoder;

        @Getter(lazy = true)
        ByteBuffer value = valueEncoder.apply(rawValue);

        public Envelope(String topic, ByteBuffer key, ByteBuffer value) {
            this.topic = topic;
            this.rawKey = key;
            this.rawValue = value;
            this.keyEncoder = this.valueEncoder = it -> (ByteBuffer) it;
        }

        public Envelope withTopic(String topic) {
            return new Envelope(
                    topic,
                    rawKey,
                    keyEncoder,
                    rawValue,
                    valueEncoder
            );
        }

        public Envelope withKey(ByteBuffer key) {
            return new Envelope(
                    topic,
                    key,
                    it -> (ByteBuffer) it,
                    rawValue,
                    valueEncoder
            );
        }

        public Envelope withValue(ByteBuffer value) {
            return new Envelope(
                    topic,
                    rawKey,
                    keyEncoder,
                    value,
                    it -> (ByteBuffer) it
            );
        }

        public <T> Envelope withKey(T rawKey, Function<T, ByteBuffer> keyEncoder) {
            return new Envelope(
                    topic,
                    rawKey,
                    (Function<Object, ByteBuffer>) keyEncoder,
                    rawValue,
                    valueEncoder
            );
        }

        public <T> Envelope withValue(T rawValue, Function<T, ByteBuffer> valueEncoder) {
            return new Envelope(
                    topic,
                    rawKey,
                    keyEncoder,
                    rawValue,
                    (Function<Object, ByteBuffer>) valueEncoder
            );
        }
    }

    @Value
    @Wither
    class Record {

        Envelope envelope;

        Instant timestamp;

        int partition;

        long offset;
    }

    interface PartitionSource {

        int getPartition();

        Publisher<Record> getPublisher();

        CompletionStage<Void> seekTo(long position);
    }
}