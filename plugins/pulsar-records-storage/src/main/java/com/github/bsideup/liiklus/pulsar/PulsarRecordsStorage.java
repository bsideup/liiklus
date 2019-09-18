package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.records.FiniteRecordsStorage;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.impl.ConsumerImplAccessor;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class PulsarRecordsStorage implements FiniteRecordsStorage {

    public static MessageId fromOffset(long offset) {
        return new MessageIdImpl(offset >>> 28, offset & 0x0F_FF_FF_FFL, -1);
    }

    public static long toOffset(MessageId messageId) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        // Combine ledger id and entry id to form offset
        // Use less than 32 bits to represent entry id since it will get
        // rolled over way before overflowing the max int range
        return (msgId.getLedgerId() << 28) | msgId.getEntryId();
    }

    private static Instant extractTime(Message<byte[]> message) {
        // event time does not always exist
        if (message.getEventTime() == 0) {
            return Instant.ofEpochMilli(message.getPublishTime());
        } else {
            return Instant.ofEpochMilli(message.getEventTime());
        }
    }

    PulsarClient pulsarClient;

    ConcurrentMap<String, Mono<Producer<byte[]>>> producers = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<OffsetInfo> publish(Envelope envelope) {
        val topic = envelope.getTopic();
        return producers
                .computeIfAbsent(topic, __ -> {
                    return Mono.fromCompletionStage(
                            pulsarClient.newProducer()
                                    .topic(topic)
                                    .hashingScheme(HashingScheme.Murmur3_32Hash)
                                    .enableBatching(false)
                                    .createAsync()
                    ).cache();
                })
                .flatMap(producer -> {
                    val valueBytes = new byte[envelope.getValue().remaining()];
                    envelope.getValue().duplicate().get(valueBytes);
                    var typedMessageBuilder = producer.newMessage()
                            .value(valueBytes);
                    var key = envelope.getKey();
                    if (key != null) {
                        typedMessageBuilder.key(StandardCharsets.UTF_8.decode(key.duplicate()).toString());
                    }

                    return Mono.fromCompletionStage(typedMessageBuilder.sendAsync())
                            .cast(MessageIdImpl.class)
                            .map(it -> new OffsetInfo(
                                    topic,
                                    it.getPartitionIndex(),
                                    toOffset(it)
                            ));
                })
                .toFuture();
    }

    @Override
    public Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset) {
        return new PulsarSubscription(topic, groupName, autoOffsetReset);
    }

    @Override
    public CompletionStage<Map<Integer, Long>> getEndOffsets(String topic) {
        return Mono
                .fromCompletionStage(() -> pulsarClient.getPartitionsForTopic(topic))
                .flatMapIterable(it -> it)
                .flatMap(partitionTopic -> {
                    var partitionIndex = TopicName.getPartitionIndex(partitionTopic);

                    var consumerFuture = pulsarClient.newConsumer()
                            .subscriptionName(UUID.randomUUID().toString())
                            .subscriptionType(SubscriptionType.Failover)
                            .topic(partitionTopic)
                            .subscribeAsync();

                    return Mono.usingWhen(
                            Mono.fromCompletionStage(() -> consumerFuture),
                            consumer -> {
                                return Mono
                                        .fromCompletionStage(() -> ConsumerImplAccessor.getLastMessageIdAsync(consumer))
                                        .map(messageId -> Map.entry(partitionIndex, toOffset(messageId)));
                            },
                            consumer -> Mono.fromCompletionStage(consumer.closeAsync()),
                            consumer -> Mono.fromCompletionStage(consumer.closeAsync())
                    );
                })
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .toFuture();
    }

    @Value
    private class PulsarSubscription implements Subscription {

        String topic;

        String groupName;

        Optional<String> autoOffsetReset;

        @Override
        public Publisher<Stream<? extends PartitionSource>> getPublisher(
                Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider
        ) {
            return Mono
                    .defer(() -> Mono.fromCompletionStage(pulsarClient.getPartitionsForTopic(topic)))
                    .map(List::size)
                    .mergeWith(Flux.never()) // Never complete
                    .map(numberOfPartitions -> {
                        return IntStream.range(0, numberOfPartitions).mapToObj(partition -> new PulsarPartitionSource(
                                topic,
                                partition,
                                groupName,
                                autoOffsetReset,
                                Mono
                                        .fromCompletionStage(offsetsProvider)
                                        .handle((it, sink) -> {
                                            if (it.containsKey(partition)) {
                                                sink.next(it.get(partition));
                                            } else {
                                                sink.complete();
                                            }
                                        })
                        ));
                    });
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }

    @Value
    @RequiredArgsConstructor
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    @ToString(of = {"topic", "partition"})
    private class PulsarPartitionSource implements PartitionSource {

        String topic;

        @Getter
        int partition;

        String groupName;

        Optional<String> autoOffsetReset;

        Mono<Long> initialOffset;

        @Override
        public Publisher<Record> getPublisher() {
            return Flux.usingWhen(
                    Mono.fromCompletionStage(() -> {
                        val consumerBuilder = pulsarClient.newConsumer()
                                .acknowledgmentGroupTime(0, TimeUnit.SECONDS) // we don't ack here at all
                                .subscriptionName(groupName)
                                .subscriptionType(SubscriptionType.Failover)
                                .topic(TopicName.get(topic).getPartition(partition).toString());

                        autoOffsetReset
                                .map(it -> {
                                    switch (it) {
                                        case "earliest":
                                            return SubscriptionInitialPosition.Earliest;
                                        case "latest":
                                            return SubscriptionInitialPosition.Latest;
                                        default:
                                            return null;
                                    }
                                })
                                .ifPresent(consumerBuilder::subscriptionInitialPosition);

                        return consumerBuilder.subscribeAsync();
                    }),
                    consumer -> {
                        return Mono
                                .fromCompletionStage(consumer::receiveAsync)
                                .repeat()
                                .onErrorResume(AlreadyClosedException.class, __ -> Mono.empty())
                                .map(message -> {
                                    var key = message.getKey();
                                    return new Record(
                                            new Envelope(
                                                    topic,
                                                    key != null ? ByteBuffer.wrap(key.getBytes()) : null,
                                                    ByteBuffer.wrap(message.getValue())
                                            ),
                                            extractTime(message),
                                            partition,
                                            toOffset(message.getMessageId())
                                    );
                                })
                                .delaySubscription(
                                        initialOffset.flatMap(offset -> {
                                            return Mono.fromCompletionStage(consumer.seekAsync(fromOffset(offset)));
                                        })
                                );
                    },
                    consumer -> Mono.fromCompletionStage(consumer.closeAsync())
            );
        }
    }
}
