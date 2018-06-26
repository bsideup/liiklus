package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.reactivestreams.Processor;
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
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
public class KafkaRecordsStorage implements RecordsStorage {

    String bootstrapServers;

    PositionsStorage positionsStorage;

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
    public Subscription subscribe(String topic, String groupId, Optional<Integer> groupVersion, Optional<String> autoOffsetReset) {
        return () -> Flux.create(assignmentsSink -> {
            val revocations = new ConcurrentHashMap<Integer, Processor<TopicPartition, TopicPartition>>();

            val receiverRef = new AtomicReference<DefaultKafkaReceiver<ByteBuffer, ByteBuffer>>();
            val recordsFluxRef = new AtomicReference<Flux<Record>>();

            val receiverOptions = createReceiverOptions(groupId, groupVersion, autoOffsetReset)
                    .subscription(singletonList(topic))
                    .addRevokeListener(partitions -> {
                        for (val partition : partitions) {
                            val topicPartition = partition.topicPartition();
                            revocations.get(topicPartition.partition()).onNext(topicPartition);
                        }
                    })
                    .addAssignListener(partitions -> {
                        val lastAckedOffsets = Mono
                                .fromCompletionStage(
                                        positionsStorage
                                                .fetch(
                                                        topic,
                                                        groupId,
                                                        partitions.stream().map(it -> it.topicPartition().partition()).collect(Collectors.toSet())
                                                )
                                )
                                .defaultIfEmpty(emptyMap())
                                .cache();

                        val kafkaReceiver = receiverRef.get();
                        val recordFlux = recordsFluxRef.get();

                        for (val partition : partitions) {
                            DefaultKafkaReceiverAccessor.pause(kafkaReceiver, partition.topicPartition());

                            val topicPartition = partition.topicPartition();
                            val partitionList = Arrays.asList(topicPartition);
                            val partitionNum = topicPartition.partition();

                            val requests = new AtomicLong();

                            val revocation = ReplayProcessor.<TopicPartition>create(1);
                            revocations.put(partitionNum, revocation);

                            assignmentsSink.next(
                                    new DelegatingGroupedPublisher<>(
                                            partitionNum,
                                            lastAckedOffsets
                                                    .delayUntil(offsets -> {
                                                        val lastAckedOffset = offsets.get(partition.topicPartition().partition());
                                                        if (lastAckedOffset != null) {
                                                            return kafkaReceiver.doOnConsumer(consumer -> {
                                                                consumer.seek(topicPartition, lastAckedOffset + 1);
                                                                return true;
                                                            });
                                                        } else {
                                                            return Mono.empty();
                                                        }
                                                    })
                                                    .thenMany(Flux.defer(() -> recordFlux
                                                            .filter(it -> it.getPartition() == partitionNum)
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
                                                    ))
                                                    .takeUntilOther(revocation)
                                    )
                            );
                        }
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
                                    record.offset(),
                                    false // TODO
                            ))
                            .share()
            );

            val disposable = recordsFluxRef.get().subscribe(
                    __ -> {},
                    assignmentsSink::error,
                    assignmentsSink::complete
            );

            assignmentsSink.onDispose(() -> {
                disposable.dispose();
                DefaultKafkaReceiverAccessor.close(kafkaReceiver);
            });
        });
    }

    protected ReceiverOptions<ByteBuffer, ByteBuffer> createReceiverOptions(String groupId, Optional<Integer> groupVersion, Optional<String> autoOffsetReset) {
        val props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + groupVersion.map(it -> "-v" + it).orElse(""));
        autoOffsetReset.ifPresent(it -> props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, it));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "0");
        return ReceiverOptions.create(props);
    }
}
