package com.github.bsideup.liiklus.positions.redis;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import lombok.Getter;
import org.testcontainers.containers.GenericContainer;

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

}
