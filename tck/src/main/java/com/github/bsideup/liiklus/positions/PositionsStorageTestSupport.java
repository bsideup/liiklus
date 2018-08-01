package com.github.bsideup.liiklus.positions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface PositionsStorageTestSupport {

    PositionsStorage getStorage();

    default <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
