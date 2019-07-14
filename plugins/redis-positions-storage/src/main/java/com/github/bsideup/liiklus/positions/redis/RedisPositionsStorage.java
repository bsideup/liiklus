package com.github.bsideup.liiklus.positions.redis;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class RedisPositionsStorage implements PositionsStorage, Closeable {

    private static final String KEY_SEPARATOR = ":";

    private static Map<Integer, Long> toPositions(Map<String, String> rawPositions) {
        return rawPositions.entrySet().stream().collect(Collectors.toMap(
                it -> Integer.parseInt(it.getKey()),
                it -> Long.parseLong(it.getValue())
        ));
    }

    Mono<StatefulRedisConnection<String, String>> connection;

    String prefix;

    @Override
    public Publisher<Positions> findAll() {
        return connection.flatMapMany(conn -> {
            return conn.reactive()
                    .keys(prefix + "*")
                    .flatMap(key -> {
                        var keyParts = sanitizeKey(key).split(KEY_SEPARATOR);
                        var topicName = keyParts[0];
                        var groupId = GroupId.ofString(keyParts[1]);
                        return conn.reactive()
                                .hgetall(key)
                                .map(rawPositions -> new Positions(
                                        topicName,
                                        groupId,
                                        toPositions(rawPositions)
                                ));
                    });
        });
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        return connection.flatMap(conn -> {
            return conn.reactive()
                    .hgetall(toKey(topic, groupId))
                    .map(RedisPositionsStorage::toPositions);
        }).toFuture();
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        return connection.flatMap(conn -> {
            return conn.reactive()
                    .keys(toKey(topic, groupName) + KEY_SEPARATOR + "*")
                    .flatMap(key -> {
                        var parts = sanitizeKey(key).split(KEY_SEPARATOR);
                        int version = Integer.parseInt(parts[parts.length - 1]);
                        return conn.reactive()
                                .hgetall(key)
                                .map(it -> Map.entry(version, toPositions(it)));
                    })
                    .collectMap(Map.Entry::getKey, Map.Entry::getValue);
        }).toFuture();
    }

    @Override
    public CompletionStage<Void> update(String topic, GroupId groupId, int partition, long position) {
        return connection.flatMap(conn -> {
            return conn.reactive()
                    .hmset(
                            toKey(topic, groupId),
                            Map.of(Integer.toString(partition), Long.toString(position))
                    )
                    .then();
        }).toFuture();
    }

    @Override
    public void close() {
        connection.subscribe(StatefulConnection::closeAsync);
    }

    private String sanitizeKey(String key) {
        return key.substring(prefix.length());
    }

    private String toKey(String topic, GroupId groupId) {
        return toKey(topic, groupId.getName()) + KEY_SEPARATOR + groupId.getVersion().orElse(0);
    }

    private String toKey(String topic, String groupName) {
        return prefix + topic + KEY_SEPARATOR + groupName;
    }
}
