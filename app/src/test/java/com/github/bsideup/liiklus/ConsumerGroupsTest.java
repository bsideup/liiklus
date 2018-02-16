package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerGroupsTest extends AbstractIntegrationTest {

    // Generate a set of keys where each key goes to unique partition
    private static Set<String> PARTITION_UNIQUE_KEYS = Mono.fromCallable(() -> UUID.randomUUID().toString())
            .repeat()
            .distinct(key -> Utils.toPositive(Utils.murmur2(key.getBytes())) % NUM_PARTITIONS)
            .take(NUM_PARTITIONS)
            .collect(Collectors.toSet())
            .block(Duration.ofSeconds(10));

    SubscribeRequest subscribeRequest;

    @Before
    public void setUpConsumerGroupsTest() throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(testName.getMethodName())
                .setGroup(testName.getMethodName())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        // Will create a topic and initialize every partition
        Flux.fromIterable(PARTITION_UNIQUE_KEYS)
                .flatMap(key -> stub
                        .publish(Mono.just(
                                PublishRequest.newBuilder()
                                        .setTopic(subscribeRequest.getTopic())
                                        .setKey(ByteString.copyFromUtf8(key))
                                        .setValue(ByteString.copyFromUtf8("bar"))
                                        .build()
                        ))
                )
                .blockLast();
    }

    @Test
    public void testConsumerGroups() {
        Map<String, Collection<SubscribeReply>> assignments = Flux
                .merge(
                        stub.subscribe(Mono.just(subscribeRequest)),
                        stub.subscribe(Mono.just(subscribeRequest))
                )
                .distinct(it -> it.getAssignment().getPartition())
                .take(NUM_PARTITIONS)
                .collectMultimap(it -> it.getAssignment().getSessionId())
                .block(Duration.ofSeconds(30));

        assertThat(assignments)
                .hasSize(2)
                .allSatisfy((__, value) -> assertThat(value).isNotEmpty());
    }
}
