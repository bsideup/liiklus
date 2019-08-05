package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.records.FiniteRecordsStorage;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public class PulsarFiniteRecordsStorage extends PulsarRecordsStorage implements FiniteRecordsStorage {

    private final PulsarAdmin pulsarAdmin;

    public PulsarFiniteRecordsStorage(PulsarClient pulsarClient, PulsarAdmin pulsarAdmin) {
        super(pulsarClient);
        this.pulsarAdmin = pulsarAdmin;
    }

    @Override
    public CompletionStage<Map<Integer, Long>> getEndOffsets(String topic) {
        var topics = (TopicsImpl) pulsarAdmin.topics();
        return Mono.fromCompletionStage(() -> topics.getPartitionedTopicMetadataAsync(topic))
                .flatMapMany(it -> {
                    return Flux
                            .range(0, it.partitions)
                            .flatMap(partition -> {
                                var partitionedTopic = TopicName.get(topic).getPartition(partition).toString();
                                return Mono
                                        .fromCompletionStage(topics.getLastMessageIdAsync(partitionedTopic))
                                        .map(messageId -> Map.entry(partition, toOffset(messageId)));
                            });
                })
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .toFuture();
    }
}
