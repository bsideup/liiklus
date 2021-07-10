package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Flux;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

class ConsumerGroupsTest extends AbstractIntegrationTest {

    SubscribeRequest subscribeRequest;

    @BeforeEach
    void setUpConsumerGroupsTest(TestInfo info) throws Exception {
        subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(info.getTestMethod().map(Method::getName).orElse("unknown"))
                .setGroup(info.getTestMethod().map(Method::getName).orElse("unknown"))
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        // Will create a topic and initialize every partition
        Flux.fromIterable(PARTITION_UNIQUE_KEYS)
                .flatMap(key -> stub
                        .publish(
                                PublishRequest.newBuilder()
                                        .setTopic(subscribeRequest.getTopic())
                                        .setKey(ByteString.copyFromUtf8(key))
                                        .setLiiklusEvent(LIIKLUS_EVENT_EXAMPLE)
                                        .build()
                        )
                )
                .blockLast();
    }

    @Test
    void testConsumerGroups() {
        Flux
                .merge(
                        stub.subscribe(subscribeRequest),
                        stub.subscribe(subscribeRequest)
                )
                .scanWith(
                        () -> new HashMap<String, Set<Integer>>(),
                        (acc, it) -> {
                            acc.computeIfAbsent(it.getAssignment().getSessionId(), __ -> new HashSet<>()).add(it.getAssignment().getPartition());
                            return acc;
                        }
                )
                .filter(it -> it.size() == 2 && it.values().stream().noneMatch(Set::isEmpty))
                .blockFirst(Duration.ofSeconds(30));
    }
}
