package com.github.bsideup.liiklus.records;

import lombok.Value;
import lombok.experimental.Wither;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
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
    @Wither
    class Envelope {

        String topic;

        ByteBuffer key;

        ByteBuffer value;
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