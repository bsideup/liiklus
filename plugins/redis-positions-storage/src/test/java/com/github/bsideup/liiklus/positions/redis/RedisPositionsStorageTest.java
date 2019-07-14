package com.github.bsideup.liiklus.positions.redis;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage.Positions;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RedisPositionsStorageTest implements PositionsStorageTests {

    static final GenericContainer redis = new GenericContainer("redis:3.0.6")
            .withExposedPorts(6379);

    static {
        redis.start();
    }

    static final RedisClient redisClient = RedisClient.create(
            RedisURI.builder()
                    .withHost(redis.getContainerIpAddress())
                    .withPort(redis.getMappedPort(6379))
                    .build()
    );

    static final RedisCommands<String, String> redisCommands = redisClient.connect().sync();

    @Getter
    PositionsStorage storage = new RedisPositionsStorage(
            Mono.just(redisClient.connect()),
            "test:" + UUID.randomUUID()
    );

    @Test
    void should_skip_keys_without_prefix(TestInfo testInfo) {
        var topicName = getTopicName(testInfo);
        redisCommands.set("CORRUPTED_KEY", "SUSPICIOUS_VALUE");
        storage.update(topicName, GroupId.ofString("mygroup-v1"), 0, 0);

        var positions = Flux.from(storage.findAll())
                .collectList()
                .block(Duration.ofSeconds(5));

        assertThat(positions)
                .extracting(Positions::getTopic)
                .containsOnly(topicName);
    }

    private String getTopicName(TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName() + "topic_name";
    }
}
