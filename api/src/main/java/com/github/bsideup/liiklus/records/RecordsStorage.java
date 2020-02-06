package com.github.bsideup.liiklus.records;

import lombok.*;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface RecordsStorage {

    CompletionStage<OffsetInfo> publish(Envelope envelope);

    Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset);

    interface Subscription {

        Publisher<Stream<? extends PartitionSource>> getPublisher(
                Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider
        );
    }

    default Envelope toEnvelope(
            String topic,
            ByteBuffer keyBuffer,
            ByteBuffer valueBuffer,
            Map<String, String> headers
    ) {
        ByteBuffer key = keyBuffer != null ? keyBuffer.asReadOnlyBuffer() : null;
        String specVersion = headers.get("ce_specversion");
        if (specVersion == null) {
            return new Envelope(
                    topic,
                    key,
                    valueBuffer.asReadOnlyBuffer()
            );
        }

        switch (specVersion) {
            case "1.0":
                return new Envelope(
                        topic,

                        key,
                        it -> it,

                        LiiklusCloudEvent.of(valueBuffer, headers),
                        LiiklusCloudEvent::asJson
                );
            default:
                throw new IllegalStateException("Unsupported CloudEvents version: " + specVersion);
        }
    }

    @Value
    class OffsetInfo {

        String topic;

        int partition;

        long offset;
    }

    @Getter
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    @ToString
    @EqualsAndHashCode
    final class Envelope {

        @With
        String topic;

        Object rawKey;

        Function<Object, ByteBuffer> keyEncoder;

        @Getter(lazy = true)
        ByteBuffer key = keyEncoder.apply(rawKey);

        Object rawValue;

        Function<Object, ByteBuffer> valueEncoder;

        @Getter(lazy = true)
        @Deprecated
        ByteBuffer value = valueEncoder.apply(rawValue);

        public Envelope(String topic, ByteBuffer key, ByteBuffer value) {
            this.topic = topic;
            this.rawKey = key;
            this.rawValue = value;
            this.keyEncoder = this.valueEncoder = it -> (ByteBuffer) it;
        }

        public <K, V> Envelope(String topic, K rawKey, Function<K, ByteBuffer> keyEncoder, V rawValue, Function<V, ByteBuffer> valueEncoder) {
            this.topic = topic;
            this.rawKey = rawKey;
            this.keyEncoder = (Function) keyEncoder;
            this.rawValue = rawValue;
            this.valueEncoder = (Function) valueEncoder;
        }

        public Envelope withKey(ByteBuffer key) {
            return new Envelope(
                    topic,
                    key,
                    it -> it,
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
                    it -> it
            );
        }

        public <T> Envelope withKey(T rawKey, Function<T, ByteBuffer> keyEncoder) {
            return new Envelope(
                    topic,
                    rawKey,
                    keyEncoder,
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
                    valueEncoder
            );
        }
    }

    @Value
    @With
    class Record {

        Envelope envelope;

        Instant timestamp;

        int partition;

        long offset;
    }

    interface PartitionSource {

        int getPartition();

        Publisher<Record> getPublisher();
    }
}