package com.github.bsideup.liiklus.positions.redis;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
public class RedisPositionsStorage implements PositionsStorage {
    private static final String KV_SEPARATOR = ":";

    private static final Collector<Map.Entry<String, String>, ?, Map<Integer, Long>> ENTRY_MAP_COLLECTOR = Collectors.toMap(
            o -> {
                String   key   = o.getKey();
                String[] split = key.split(KV_SEPARATOR);
                return Integer.parseInt(split[1]);
            },
            o -> Long.parseLong(o.getValue())
    );

    private final StatefulRedisConnection<String, String> connection;

    public RedisPositionsStorage(RedisClient redisClient) {
        connection = redisClient.connect();
    }

    @Override
    public Publisher<Positions> findAll() {
        return connection.reactive()
                .keys("*")
                .flatMap(s -> connection.reactive().hgetall(s)
                        .map(stringStringMap -> {
                            String[] split = s.split(KV_SEPARATOR);
                            return new Positions(split[0], GroupId.ofString(split[1]), stringStringMap
                                    .entrySet().stream().collect(ENTRY_MAP_COLLECTOR));
                        })
                );
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        return connection.async()
                .hgetall(toKey(topic, groupId.asString()))
                .thenApply(stringStringMap -> stringStringMap.entrySet().stream()
                        .collect(ENTRY_MAP_COLLECTOR));
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        return connection.reactive()
                .keys(toKey(topic, groupName) + "*")
                .flatMap(s -> connection.reactive()
                        .hgetall(s)
                        .map(stringStringMap -> stringStringMap.entrySet()
                                .stream().collect(ENTRY_MAP_COLLECTOR))
                        .map(integerLongMap -> new VersionWithData(
                                GroupId.ofString(s.split(":")[1]).getVersion().orElse(0),
                                integerLongMap)
                        )
                )
                .collectMap(
                        VersionWithData::getVersion,
                        VersionWithData::getPositionByPratition
                ).toFuture();
    }

    @Override
    public CompletionStage<Void> update(String topic, GroupId groupId, int partition, long position) {
        return connection.async()
                .hmset(
                        toKey(topic, groupId.asString()),
                        ImmutableMap.of(
                                groupId.getVersion().orElse(0).toString() + KV_SEPARATOR + partition,
                                Long.toString(position)
                        )
                ).thenAccept(s -> log.debug("set is {} for {}", s, topic));
    }

    /**
     * Should close connection after stop context
     */
    public void shutdown() {
        connection.close();
    }

    private static String toKey(String topic, String groupName) {
        return topic + KV_SEPARATOR + groupName;
    }

    @Value
    @RequiredArgsConstructor(staticName = "of")
    private static class VersionWithData {
        int                version;
        Map<Integer, Long> positionByPratition;
    }

}
