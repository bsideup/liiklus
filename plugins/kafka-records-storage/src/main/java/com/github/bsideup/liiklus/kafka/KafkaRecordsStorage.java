package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.receiver.internals.DefaultKafkaReceiverAccessor;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
public class KafkaRecordsStorage implements RecordsStorage {

    String bootstrapServers;

    KafkaSender<ByteBuffer, ByteBuffer> sender;

    @Override
    public CompletionStage<OffsetInfo> publish(Envelope envelope) {
        String topic = envelope.getTopic();
        return sender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, envelope.getKey(), envelope.getValue()), UUID.randomUUID())))
                .single()
                .<OffsetInfo>handle((it, sink) -> {
                    if (it.exception() != null) {
                        sink.error(it.exception());
                    } else {
                        sink.next(new OffsetInfo(topic, it.recordMetadata().partition(), it.recordMetadata().offset()));
                    }
                })
                .toFuture();
    }

    @Override
    public Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset) {
        return new KafkaSubscription(topic, groupName, autoOffsetReset);
    }

    protected ReceiverOptions<ByteBuffer, ByteBuffer> createReceiverOptions(String groupName, Optional<String> autoOffsetReset) {
        val props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        autoOffsetReset.ifPresent(it -> props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, it));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "0");
        return ReceiverOptions.create(props);
    }

    @Value
    private class KafkaSubscription implements Subscription {

        String topic;

        String groupName;

        Optional<String> autoOffsetReset;

        @Override
        public Publisher<Stream<? extends PartitionSource>> getPublisher() {
            return Flux.create(assignmentsSink -> {
                val revocations = new ConcurrentHashMap<Integer, Processor<TopicPartition, TopicPartition>>();

                val receiverRef = new AtomicReference<DefaultKafkaReceiver<ByteBuffer, ByteBuffer>>();
                val recordsFluxRef = new AtomicReference<Flux<Record>>();

                val receiverOptions = createReceiverOptions(groupName, autoOffsetReset)
                        .subscription(singletonList(topic))
                        .addRevokeListener(partitions -> {
                            for (val partition : partitions) {
                                val topicPartition = partition.topicPartition();
                                revocations.get(topicPartition.partition()).onNext(topicPartition);
                            }
                        })
                        .addAssignListener(partitions -> {
                            val kafkaReceiver = receiverRef.get();
                            val recordFlux = recordsFluxRef.get();

                            for (val partition : partitions) {
                                DefaultKafkaReceiverAccessor.pause(kafkaReceiver, partition.topicPartition());
                            }

                            assignmentsSink.next(
                                    partitions.stream()
                                            .map(partition -> {
                                                val topicPartition = partition.topicPartition();
                                                val partitionSource = new KafkaPartitionSource(recordFlux, kafkaReceiver, topicPartition);
                                                revocations.put(topicPartition.partition(), partitionSource.getRevocation());
                                                return partitionSource;
                                            })
                            );
                        });

                val kafkaReceiver = (DefaultKafkaReceiver<ByteBuffer, ByteBuffer>) KafkaReceiver.create(receiverOptions);
                receiverRef.set(kafkaReceiver);

                recordsFluxRef.set(
                        Flux
                                .defer(kafkaReceiver::receive)
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
                                .share()
                );

                val disposable = recordsFluxRef.get().subscribe(
                        null,
                        assignmentsSink::error,
                        assignmentsSink::complete
                );

                assignmentsSink.onDispose(() -> {
                    disposable.dispose();
                    DefaultKafkaReceiverAccessor.close(kafkaReceiver);
                });
            });
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }

    @Value
    @ToString(of = "topicPartition")
    private static class KafkaPartitionSource implements PartitionSource {

        Flux<Record> recordFlux;

        DefaultKafkaReceiver<ByteBuffer, ByteBuffer> kafkaReceiver;

        TopicPartition topicPartition;

        ReplayProcessor<TopicPartition> revocation = ReplayProcessor.create(1);

        AtomicLong requests = new AtomicLong();

        @Override
        public int getPartition() {
            return topicPartition.partition();
        }

        @Override
        public Publisher<Record> getPublisher() {
            val partitionList = Arrays.asList(topicPartition);
            return recordFlux
                    .filter(it -> it.getPartition() == topicPartition.partition())
                    .delayUntil(record -> {
                        if (requests.decrementAndGet() < 0) {
                            return kafkaReceiver.doOnConsumer(consumer -> {
                                if (requests.get() < 0) {
                                    consumer.pause(partitionList);
                                }
                                return true;
                            });
                        } else {
                            return Mono.empty();
                        }
                    })
                    .doOnRequest(requested -> {
                        if (requests.addAndGet(requested) > 0) {
                            DefaultKafkaReceiverAccessor.resume(kafkaReceiver, topicPartition);
                        }
                    })
                    .takeUntilOther(revocation)
                    .doFinally(__ -> DefaultKafkaReceiverAccessor.pause(kafkaReceiver, topicPartition));
        }

        @Override
        public CompletionStage<Void> seekTo(long position) {
            return kafkaReceiver
                    .doOnConsumer(consumer -> {
                        consumer.seek(topicPartition, position);
                        return true;
                    })
                    .then()
                    .toFuture();
        }
    }
}
