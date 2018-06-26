package com.github.bsideup.liiklus.inmemory;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Set;
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
                        entry.getKey().getGroupId(),
                        entry.getValue()
                ));
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, String groupId) {
        return CompletableFuture.completedFuture(storage.get(Key.of(topic, groupId)));
    }

    @Override
    public CompletionStage<Map<Integer, Long>> fetch(String topic, String groupId, Set<Integer> __) {
        return findAll(topic, groupId);
    }

    @Override
    public Publisher<Positions> findByPrefix(String topic, String groupPrefix) {
        return Flux.from(findAll())
                .filter(it -> topic.equals(it.getTopic()))
                .filter(it -> it.getGroupId().startsWith(groupPrefix));
    }

    @Override
    public CompletionStage<Void> update(String topic, String groupId, int partition, long position) {

        storage.computeIfAbsent(Key.of(topic, groupId), __ -> new ConcurrentHashMap<>()).put(partition, position);

        return CompletableFuture.completedFuture(null);
    }

    @Value
    @RequiredArgsConstructor(staticName = "of")
    private static class Key {
        String topic;

        String groupId;
    }
}
