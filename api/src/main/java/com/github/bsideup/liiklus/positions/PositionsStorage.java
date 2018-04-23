package com.github.bsideup.liiklus.positions;

import lombok.Value;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public interface PositionsStorage {

    CompletionStage<Map<Integer, Long>> fetch(String topic, String groupId, Set<Integer> partitions);

    CompletionStage<Void> update(String topic, String groupId, int partition, long position);

    Publisher<Positions> findAll();

    CompletionStage<Map<Integer, Long>> findAll(String topic, String groupId);

    @Value
    class Positions {

        String topic;

        String groupId;

        Map<Integer, Long> values;
    }
}
