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
    private static final String KV_SEPARATOR      = ":";
    private static final String REDIS_KEYS_PREFIX = "liiklus.positions.key-";

    private static final Collector<Map.Entry<String, String>, ?, Map<Integer, Long>> ENTRY_MAP_COLLECTOR = Collectors.toMap(
            o -> Integer.parseInt(o.getKey()),
            o -> Long.parseLong(o.getValue())
    );

    private final StatefulRedisConnection<String, String>        connection;

    public RedisPositionsStorage(RedisClient redisClient) {
        connection = redisClient.connect();
    }

    @Override
    public Publisher<Positions> findAll() {
        return connection.reactive()
                .keys(REDIS_KEYS_PREFIX + "*")
                .flatMap(s -> connection.reactive().hgetall(s)
                        .map(stringStringMap -> {
                            String[] split     = s.split(KV_SEPARATOR);
                            String   topicName = split[0].replace(REDIS_KEYS_PREFIX, "");
                            GroupId  groupId   = GroupId.ofString(split[1]);
                            return new Positions(
                                    topicName,
                                    groupId,
                                    toIntLongMap(stringStringMap)
                            );
                        })
                )
                .onErrorContinue((throwable, o) -> log.debug("Error when handling redis response {}", throwable.getMessage()));
    }

    private Map<Integer, Long> toIntLongMap(Map<String, String> stringStringMap) {
        return stringStringMap.entrySet().stream().collect(ENTRY_MAP_COLLECTOR);
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        return connection.async()
                .hgetall(toKey(topic, groupId.asString()))
                .thenApply(this::toIntLongMap);
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        return connection.reactive()
                .keys(toKey(topic, groupName) + "*")
                .flatMap(s -> connection.reactive()
                        .hgetall(s)
                        .map(this::toIntLongMap)
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
                                Integer.toString(partition),
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
        return REDIS_KEYS_PREFIX + topic + KV_SEPARATOR + groupName;
    }

    @Value
    @RequiredArgsConstructor(staticName = "of")
    private static class VersionWithData {
        int                version;
        Map<Integer, Long> positionByPratition;
    }

}
