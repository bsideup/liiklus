package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.records.FiniteRecordsStorage;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@FieldDefaults(makeFinal = true)
@Slf4j
public class KafkaRecordsStorage implements FiniteRecordsStorage {

    private static final Scheduler KAFKA_POLL_SCHEDULER = Schedulers.elastic();

    String bootstrapServers;

    private final KafkaProducer<ByteBuffer, ByteBuffer> producer;

    public KafkaRecordsStorage(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "liiklus-" + UUID.randomUUID().toString());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(
                props,
                new ByteBufferSerializer(),
                new ByteBufferSerializer()
        );
    }

    @Override
    public CompletionStage<Map<Integer, Long>> getEndOffsets(String topic) {
        return Mono.fromCallable(() -> {
            var properties = new HashMap<String, Object>();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "0");
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

            try (
                    var consumer = new KafkaConsumer<ByteBuffer, ByteBuffer>(
                            properties,
                            new ByteBufferDeserializer(),
                            new ByteBufferDeserializer()
                    )
            ) {
                consumer.subscribe(List.of(topic));

                var endOffsets = consumer.endOffsets(
                        consumer.partitionsFor(topic).stream()
                                .map(it -> new TopicPartition(topic, it.partition()))
                                .collect(Collectors.toSet())
                );

                return endOffsets.entrySet().stream().collect(Collectors.toMap(
                        it -> it.getKey().partition(),
                        it -> it.getValue() - 1
                ));
            }
        }).subscribeOn(Schedulers.elastic()).toFuture();
    }

    @Override
    public CompletionStage<OffsetInfo> publish(Envelope envelope) {
        String topic = envelope.getTopic();

        var producerRecord = new ProducerRecord<ByteBuffer, ByteBuffer>(topic, envelope.getKey(), envelope.getValue());

        return Mono.<OffsetInfo>create(sink -> {
            var future = producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    sink.error(exception);
                } else {
                    sink.success(new OffsetInfo(
                            topic,
                            metadata.partition(),
                            metadata.offset()
                    ));
                }
            });

            sink.onCancel(() -> future.cancel(true));
        }).toFuture();
    }

    @Override
    public Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset) {
        return new KafkaSubscription(topic, groupName, autoOffsetReset);
    }

    @Value
    private class KafkaSubscription implements Subscription {

        String topic;

        String groupName;

        Optional<String> autoOffsetReset;

        @Override
        public Publisher<Stream<? extends PartitionSource>> getPublisher(
                Supplier<CompletionStage<Map<Integer, Long>>> offsetsProvider
        ) {
            return Flux.create(sink -> {
                try {
                    var properties = new HashMap<String, Object>();
                    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
                    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "0");
                    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Queues.SMALL_BUFFER_SIZE + "");
                    autoOffsetReset.ifPresent(it -> properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, it));

                    var consumer = new KafkaConsumer<ByteBuffer, ByteBuffer>(
                            properties,
                            new ByteBufferDeserializer(),
                            new ByteBufferDeserializer()
                    );

                    var topics = Arrays.asList(topic);

                    if (!sink.isCancelled()) {
                        var pausedPartitions = new ConcurrentHashMap<TopicPartition, Boolean>();

                        var records = Flux
                                .<ConsumerRecords<ByteBuffer, ByteBuffer>>create(recordsSink -> {
                                    try {
                                        while (!sink.isCancelled() && !recordsSink.isCancelled()) {
                                            for (var entry : pausedPartitions.entrySet()) {
                                                var topicPartition = entry.getKey();
                                                if (!consumer.assignment().contains(topicPartition)) {
                                                    continue;
                                                }

                                                var partitions = Arrays.asList(topicPartition);
                                                if (entry.getValue()) {
                                                    consumer.pause(partitions);
                                                } else {
                                                    consumer.resume(partitions);
                                                }
                                            }

                                            var consumerRecords = consumer.poll(Duration.ofMillis(10));
                                            if (!consumerRecords.isEmpty()) {
                                                recordsSink.next(consumerRecords);
                                            }
                                        }

                                        try {
                                            consumer.unsubscribe();
                                        } catch (Exception e) {
                                            log.warn("Failed to unsubscribe", e);
                                        }
                                        try {
                                            consumer.close();
                                        } catch (Exception e) {
                                            log.warn("Failed to close", e);
                                        }

                                        recordsSink.complete();
                                    } catch (Exception e) {
                                        recordsSink.error(e);
                                    }
                                }, FluxSink.OverflowStrategy.ERROR)
                                .subscribeOn(KAFKA_POLL_SCHEDULER)
                                .publish(1);

                        var revocations = new ConcurrentHashMap<Integer, DirectProcessor<Boolean>>();
                        consumer.subscribe(topics, new ConsumerRebalanceListener() {

                            @Override
                            @SneakyThrows
                            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                                for (var topicPartition : partitions) {
                                    revocations.putIfAbsent(topicPartition.partition(), DirectProcessor.create());
                                    pausedPartitions.put(topicPartition, true);
                                }

                                var offsets = offsetsProvider.get().toCompletableFuture().get(10, TimeUnit.SECONDS);

                                for (var topicPartition : consumer.assignment()) {
                                    int partition = topicPartition.partition();
                                    if (offsets.containsKey(partition)) {
                                        Long offset = offsets.get(partition);
                                        consumer.seek(topicPartition, offset);
                                    }
                                }

                                sink.next(partitions.stream().map(topicPartition -> new PartitionSource() {

                                    @Override
                                    public int getPartition() {
                                        return topicPartition.partition();
                                    }

                                    @Override
                                    public Publisher<Record> getPublisher() {
                                        AtomicLong requests = new AtomicLong();
                                        return records
                                                .flatMapIterable(it -> {
                                                    var recordsOfPartition = it.records(topicPartition);
                                                    if (!recordsOfPartition.isEmpty()) {
                                                        pausedPartitions.put(topicPartition, requests.addAndGet(-recordsOfPartition.size()) <= 0);
                                                    }
                                                    return recordsOfPartition;
                                                })
                                                .doOnRequest(request -> pausedPartitions.put(topicPartition, requests.addAndGet(request) <= 0))
                                                .map(record -> new Record(
                                                        new Envelope(
                                                                topic,
                                                                record.key(),
                                                                record.value()
                                                        ),
                                                        Instant.ofEpochMilli(record.timestamp()),
                                                        record.partition(),
                                                        record.offset()
                                                ))
                                                .takeUntilOther(revocations.get(topicPartition.partition()));
                                    }
                                }));
                            }

                            @Override
                            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                                for (var partition : partitions) {
                                    revocations.get(partition.partition()).onNext(true);
                                }
                            }
                        });

                        records.connect();
                    }
                } catch (Exception e) {
                    sink.error(e);
                }
            });
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }
}
