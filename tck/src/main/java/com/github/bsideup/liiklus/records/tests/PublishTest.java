package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public interface PublishTest extends RecordStorageTestSupport {

    @Test
    default void testPublish() throws Exception {
        val record = createEnvelope("key".getBytes());

        val offsetInfo = publish(record);

        assertThat(offsetInfo)
                .satisfies(info -> {
                    assertThat(info.getTopic()).as("topic").isEqualTo(getTopic());
                    assertThat(info.getPartition()).as("partition").isNotNegative();
                    assertThat(info.getOffset()).as("offset").isNotNegative();
                });

        val receivedRecord = subscribeToPartition(offsetInfo.getPartition())
                .flatMap(RecordsStorage.PartitionSource::getPublisher)
                .blockFirst(Duration.ofSeconds(10));

        assertThat(receivedRecord.getEnvelope()).as("envelope").isEqualTo(record);
        assertThat(receivedRecord.getPartition()).as("partition").isEqualTo(offsetInfo.getPartition());
        assertThat(receivedRecord.getOffset()).as("offset").isEqualTo(offsetInfo.getOffset());
    }

    @Test
    default void testPublishMany() {
        val numRecords = 5;

        val offsetInfos = publishMany("key".getBytes(), numRecords);

        assertThat(offsetInfos).hasSize(numRecords);

        val partition = offsetInfos.get(0).getPartition();

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
