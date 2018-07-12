package com.github.bsideup.liiklus.positions;

import lombok.Value;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface PositionsStorage {

    CompletionStage<Void> update(String topic, GroupId groupId, int partition, long position);

    Publisher<Positions> findAll();

    CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId);

    CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName);

    @Value
    class Positions {

        String topic;

        GroupId groupId;

        Map<Integer, Long> values;
    }
}
