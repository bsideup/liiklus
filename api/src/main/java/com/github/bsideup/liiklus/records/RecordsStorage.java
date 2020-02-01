package com.github.bsideup.liiklus.records;

import io.cloudevents.CloudEvent;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.format.builder.HeadersStep;
import io.cloudevents.json.Json;
import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v1.http.Marshallers;
import io.cloudevents.v1.http.Unmarshallers;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

    default Wire<ByteBuffer, String, String> toWire(Envelope envelope) throws IllegalArgumentException {
        Object rawValue = envelope.getRawValue();

        if (!(rawValue instanceof CloudEvent)) {
            // TODO Add Envelope#event and make CloudEvent a fist-class citizen
            throw new IllegalArgumentException("Must be a CloudEvent!");
        }

        CloudEvent<?, ?> event = (CloudEvent<?, ?>) rawValue;

        final Wire<String, String, String> wire;
        String specVersion = event.getAttributes().getSpecversion();
        switch (specVersion) {
            case "1.0":
                wire = RecordsStorageInternal.EVENT_MARSHALLER
                        .withEvent(() -> (CloudEvent) event)
                        .marshal();
                break;
            default:
                throw new IllegalArgumentException("Unknown CloudEvents version: " + specVersion);
        }

        return new Wire(
                wire.getPayload().map(it -> ByteBuffer.wrap(it.getBytes())).orElse(null),
                wire.getHeaders()
        );
    }

    default Envelope toEnvelope(
            String topic,
            ByteBuffer keyBuffer,
            ByteBuffer valueBuffer,
            Map<String, String> headers
    ) {
        ByteBuffer key = keyBuffer != null ? keyBuffer.asReadOnlyBuffer() : null;
        String specVersion = headers.get("ce-specversion");
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
                        it -> (ByteBuffer) it,

                        RecordsStorageInternal.EVENT_UNMARSHALLER
                                .withHeaders(() -> (Map) headers)
                                .withPayload(() -> StandardCharsets.UTF_8.decode(valueBuffer.duplicate()).toString())
                                .unmarshal(),
                        it -> ByteBuffer.wrap(Json.binaryEncode(it)).asReadOnlyBuffer()
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
        @Deprecated
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

class RecordsStorageInternal {
    static final EventStep<AttributesImpl, Object, String, String> EVENT_MARSHALLER = Marshallers.binary();

    static final HeadersStep<AttributesImpl, byte[], String> EVENT_UNMARSHALLER = Unmarshallers.binary(byte[].class);
}