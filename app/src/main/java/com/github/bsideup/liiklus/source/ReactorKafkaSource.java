package com.github.bsideup.liiklus.source;

import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.receiver.internals.DefaultKafkaReceiverAccessor;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
public class ReactorKafkaSource implements KafkaSource {

    String bootstrapServers;

    @Override
    public Subscription subscribe(Map<String, Object> props, String topic) {
        Map<String, Object> finalProps = new HashMap<>(props);
        finalProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        finalProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        finalProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        finalProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        ReceiverOptions<ByteBuffer, ByteBuffer> receiverOptions = ReceiverOptions.<ByteBuffer, ByteBuffer>create(finalProps)
                .subscription(singletonList(topic));

        DefaultKafkaReceiver<ByteBuffer, ByteBuffer> kafkaReceiver = (DefaultKafkaReceiver<ByteBuffer, ByteBuffer>) KafkaReceiver.create(receiverOptions);

        return new Subscription() {

            @Override
            public Publisher<? extends GroupedPublisher<Integer, KafkaRecord>> getPublisher() {
                return Flux
                        .defer(kafkaReceiver::receive)
                        .map(record -> new KafkaRecord(
                                record.key(),
                                record.value(),
                                Instant.ofEpochMilli(record.timestamp()),
                                record.partition(),
                                record.offset()
                        ))
                        .groupBy(KafkaRecord::getPartition)
                        .map(it -> new DelegatingGroupedPublisher<>(it.key(), it))
                        .doFinally(__ -> DefaultKafkaReceiverAccessor.close(kafkaReceiver));
            }

            @Override
            public Mono<Void> acknowledge(int partition, long offset) {
                DefaultKafkaReceiverAccessor.updateOffset(
                        kafkaReceiver,
                        new TopicPartition(topic, partition),
                        offset
                );
                return Mono.empty();
            }
        };
    }
}
