package com.github.bsideup.liiklus.inmemory;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * WARNING: this storage type should only be used for testing and NOT in production
 */
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class InMemoryPositionsStorage implements PositionsStorage {

    ConcurrentMap<Key, ConcurrentMap<Integer, Long>> storage = new ConcurrentHashMap<>();

    @Override
    public Publisher<Positions> findAll() {
        return Flux.fromIterable(storage.entrySet())
                .map(entry -> new Positions(
                        entry.getKey().getTopic(),
                        GroupId.ofString(entry.getKey().getGroupId()),
                        entry.getValue()
                ));
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        return CompletableFuture.completedFuture(storage.get(Key.of(topic, groupId.asString())));
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        return Flux.fromIterable(storage.entrySet())
                .filter(it -> topic.equals(it.getKey().getTopic()))
                .filter(it -> groupName.equals(GroupId.ofString(it.getKey().getGroupId()).getName()))
                .<Integer, Map<Integer, Long>>collectMap(
                        it -> GroupId.ofString(it.getKey().getGroupId()).getVersion().orElse(0),
                        Map.Entry::getValue
                )
                .toFuture();
    }

    @Override
    public CompletionStage<Void> update(String topic, GroupId groupId, int partition, long position) {
        storage.computeIfAbsent(Key.of(topic, groupId.asString()), __ -> new ConcurrentHashMap<>()).put(partition, position);

        return CompletableFuture.completedFuture(null);
    }

    @Value
    @RequiredArgsConstructor(staticName = "of")
    private static class Key {

        String topic;

        String groupId;
    }
}
