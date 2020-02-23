package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

public interface PublishTest extends RecordStorageTestSupport {

    @Test
    default void testPublish() throws Exception {
        var envelope = createEnvelope("key".getBytes());

        var offsetInfo = publish(envelope);

        assertThat(offsetInfo)
                .satisfies(info -> {
                    assertThat(info.getTopic()).as("topic").isEqualTo(getTopic());
                    assertThat(info.getOffset()).as("offset").isNotNegative();
                });

        var receivedRecord = subscribeToPartition(offsetInfo.getPartition())
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(receivedRecord.getEnvelope()).as("envelope")
                .usingComparatorForType(Comparator.comparing(Json::encode), CloudEvent.class)
                .isEqualToIgnoringGivenFields(envelope, "keyEncoder", "valueEncoder")
                .satisfies(it -> {
                    assertThat(it.getRawValue()).isInstanceOf(CloudEvent.class);
                });

        assertThat(receivedRecord.getPartition()).as("partition").isEqualTo(offsetInfo.getPartition());
        assertThat(receivedRecord.getOffset()).as("offset").isEqualTo(offsetInfo.getOffset());
    }

    @Test
    default void testPublishMany() {
        var numRecords = 5;

        var offsetInfos = publishMany("key".getBytes(), numRecords);

        assertThat(offsetInfos).hasSize(numRecords);

        var partition = offsetInfos.get(0).getPartition();

        assertThat(offsetInfos).extracting(RecordsStorage.OffsetInfo::getPartition).containsOnly(partition);
    }

    @Test
    default void testPublishOffsetIsGrowing() {
        RecordsStorage.OffsetInfo first = publish("key".getBytes(), "value1".getBytes());
        RecordsStorage.OffsetInfo second = publish("key".getBytes(), "value2".getBytes());

        assertThat(first.getPartition()).isEqualTo(second.getPartition());
        assertThat(second.getOffset()).isGreaterThan(first.getOffset());
    }
}
