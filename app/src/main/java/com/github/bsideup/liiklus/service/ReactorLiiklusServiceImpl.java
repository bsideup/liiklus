package com.github.bsideup.liiklus.service;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.github.bsideup.liiklus.source.KafkaSource;
import com.github.bsideup.liiklus.source.KafkaSource.Subscription;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.lognet.springboot.grpc.GRpcService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
@GRpcService
public class ReactorLiiklusServiceImpl extends ReactorLiiklusServiceGrpc.LiiklusServiceImplBase {

    ConcurrentMap<String, ConcurrentMap<String, Subscription>> subscriptions = new ConcurrentHashMap<>();

    ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer, Flux<KafkaSource.KafkaRecord>>>> sources = new ConcurrentHashMap<>();

    KafkaSource kafkaSource;

    @Override
    public Mono<PublishReply> publish(Mono<PublishRequest> requestMono) {
        return requestMono
                .flatMap(request -> kafkaSource.publish(
                        request.getTopic(),
                        request.getKey().asReadOnlyByteBuffer(),
                        request.getValue().asReadOnlyByteBuffer()
                ))
                .then(Mono.just(PublishReply.getDefaultInstance()));
    }

    @Override
    public Flux<SubscribeReply> subscribe(Mono<SubscribeRequest> requestFlux) {
        return requestFlux.flatMapMany(subscribe -> {
            Map<String, Object> props = new HashMap<>();
            String groupId = subscribe.getGroup();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            switch (subscribe.getAutoOffsetReset()) {
                case EARLIEST:
                    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    break;
                case LATEST:
                    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                    break;
                case UNRECOGNIZED:
                    // ignore
            }

            String topic = subscribe.getTopic();
            Subscription subscription = kafkaSource.subscribe(props, topic);

            ConcurrentMap<String, Subscription> topicSubscriptions = subscriptions.computeIfAbsent(topic, __ -> new ConcurrentHashMap<>());
            if (topicSubscriptions.putIfAbsent(groupId, subscription) != null) {
                return Mono.error(new IllegalStateException("Only 1 subscription is allowed"));
            }

            return Flux.from(subscription.getPublisher())
                    .<SubscribeReply>handle((group, sink) -> {
                        int partition = group.getGroup();

                        ConcurrentMap<Integer, Flux<KafkaSource.KafkaRecord>> sourcesByPartition = sources
                                .computeIfAbsent(topic, __ -> new ConcurrentHashMap<>())
                                .computeIfAbsent(groupId, __ -> new ConcurrentHashMap<>());

                        sourcesByPartition.put(
                                partition,
                                Flux.from(group)
                                        .doFinally(__ -> {
                                            sourcesByPartition.remove(partition);
                                            sink.complete();
                                        })
                        );

                        sink.next(
                                SubscribeReply.newBuilder()
                                        .setAssignment(SubscribeReply.Assignment.newBuilder().setPartition(partition))
                                        .build()
                        );
                    })
                    .log("subscribe", Level.WARNING, SignalType.ON_ERROR)
                    .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                    .doFinally(__ -> topicSubscriptions.remove(groupId));
        });
    }

    @Override
    public Flux<ReceiveReply> receive(Mono<ReceiveRequest> requestMono) {
        return requestMono
                .flatMapMany(request -> {
                    String topic = request.getTopic();
                    String group = request.getGroup();
                    int partition = request.getPartition();
                    // TODO auto ack to the last known offset
                    long lastKnownOffset = request.getLastKnownOffset();

                    return sources.get(topic).get(group).get(partition)
                            .map(consumerRecord -> ReceiveReply.newBuilder()
                                    .setRecord(
                                            Record.newBuilder()
                                                    .setOffset(consumerRecord.getOffset())
                                                    .setKey(ByteString.copyFrom(consumerRecord.getKey()))
                                                    .setValue(ByteString.copyFrom(consumerRecord.getValue()))
                                                    .setTimestamp(Timestamp.newBuilder()
                                                            .setSeconds(consumerRecord.getTimestamp().getEpochSecond())
                                                            .setNanos(consumerRecord.getTimestamp().getNano())
                                                    )
                                    )
                                    .build()
                            );
                })
                .log("receive", Level.WARNING, SignalType.ON_ERROR);
    }

    @Override
    public Mono<Empty> ack(Mono<AckRequest> request) {
        return request
                .flatMap(ack -> subscriptions.get(ack.getTopic()).get(ack.getGroup()).acknowledge(ack.getPartition(), ack.getOffset()))
                .then(Mono.just(Empty.getDefaultInstance()))
                .log("ack", Level.WARNING, SignalType.ON_ERROR);
    }
}
