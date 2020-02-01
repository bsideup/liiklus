package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ConsumerGroupsTest extends AbstractIntegrationTest {

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
                .flatMap(key -> {
                    @SuppressWarnings("deprecation")
                    var publishRequest = PublishRequest.newBuilder()
                            .setTopic(subscribeRequest.getTopic())
                            .setKey(ByteString.copyFromUtf8(key))
                            .setValue(ByteString.copyFromUtf8("bar"))
                            .build();
                    return stub.publish(publishRequest);
                })
                .blockLast();
    }

    @Test
    public void testConsumerGroups() {
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
