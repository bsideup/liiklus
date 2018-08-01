package com.github.bsideup.liiklus.positions;

import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public interface PositionsStorageTestSupport {

    PositionsStorage getStorage();

    String getTopic();

    default <T> Mono<T> asDeferMono(Supplier<CompletionStage<T>> stageSupplier) {
        return Mono.defer(() -> Mono.fromCompletionStage(stageSupplier.get()));
    }

    default <T, V> Map<T, V> mapOf(T key, V value) {
        return Collections.singletonMap(key, value);
    }

    default <T, V> Map<T, V> mapOf(T key, V value, T key2, V value2) {
        HashMap<T, V> map = new HashMap<>();
        map.put(key, value);
        map.put(key2, value2);
        return map;
    }
}
