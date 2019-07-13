package com.github.bsideup.liiklus.positions.redis;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage.Positions;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import lombok.Getter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RedisPositionsStorageTest implements PositionsStorageTests {
    public static final GenericContainer redis = new GenericContainer("redis:3.0.6")
            .withExposedPorts(6379);

    static {
        redis.start();
    }

    private final RedisClient redisClient = RedisClient.create(RedisURI.builder()
            .withHost(redis.getContainerIpAddress())
            .withPort(redis.getMappedPort(6379))
            .build());

    @Getter
    PositionsStorage storage = new RedisPositionsStorage(redisClient);

    @Test
    void should_skip_keys_without_prefix(TestInfo testInfo) {
        //given
        String topicName = testInfo.getDisplayName() + "topic_name";
        redisClient.connect().sync().set("CORRUPTED_KEY", "SUSPICIOUS_VALUE");
        storage.update(topicName, GroupId.ofString("mygroup-v1"), 0, 0);

        //expect
        assertThat(
                Flux.from(storage.findAll())
                        .collectList()
                        .block(Duration.ofSeconds(5))
        )
                .extracting(Positions::getTopic)
                .contains(topicName)
                .doesNotContain("CORRUPTED_KEY");
    }

    @Test
    @DisplayName("Should skip key without group name, because only PREFIX-topic:groupname is acceptable format")
    void should_skip_unappropriated_keys(TestInfo testInfo) {
        //given
        String topic_name = testInfo.getTestMethod().get().getName() + "topic_name";
        redisClient.connect().sync().set("liiklus.positions.key-CORRUPTED_KEY", "SUSPICIOUS_VALUE");
        redisClient.connect().sync().hset("liiklus.positions.key-CORRUPTED_KEY2", "position", "SUSPICIOUS_VALUE");
        storage.update(topic_name, GroupId.ofString("mygroup-v1"), 0, 0);

        //expect
        assertThat(
                Flux.from(storage.findAll())
                        .collectList()
                        .block(Duration.ofSeconds(5))
        )
                .extracting(Positions::getTopic)
                .contains(topic_name)
                .doesNotContain("CORRUPTED_KEY", "CORRUPTED_KEY2");
    }
}
