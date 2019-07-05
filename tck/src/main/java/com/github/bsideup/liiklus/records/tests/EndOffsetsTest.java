package com.github.bsideup.liiklus.records.tests;

import com.github.bsideup.liiklus.records.FiniteRecordsStorage;
import com.github.bsideup.liiklus.records.RecordStorageTestSupport;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public interface EndOffsetsTest extends RecordStorageTestSupport {

    default FiniteRecordsStorage getFiniteTarget() {
        return (FiniteRecordsStorage) getTarget();
    }

    int getNumberOfPartitions();

    String keyByPartition(int partition);

    @BeforeEach
    default void blah(TestInfo testInfo) {
        if (EndOffsetsTest.class == testInfo.getTestMethod().map(Method::getDeclaringClass).orElse(null)) {
            Assumptions.assumeTrue(getTarget() instanceof FiniteRecordsStorage, "target is finite");
        }
    }

    @Test
    default void testEndOffsets() throws Exception {
        var topic = getTopic();

        for (int partition = 0; partition < getNumberOfPartitions(); partition++) {
            for (int i = 0; i < partition + 1; i++) {
                publish(keyByPartition(partition).getBytes(), new byte[1]);
            }
        }

        var offsets = getFiniteTarget().getEndOffsets(topic).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(offsets)
                .hasSize(getNumberOfPartitions())
                .allSatisfy((partition, offset) -> {
                    assertThat(offset)
                            .as("offset of p" + partition)
                            .isEqualTo(partition.longValue());
                });
    }

    @Test
    default void testEndOffsets_unknownTopic() throws Exception {
        var topic = UUID.randomUUID().toString();

        var offsets = getFiniteTarget().getEndOffsets(topic).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(offsets)
                .allSatisfy((partition, offset) -> {
                    assertThat(offset)
                            .as("offset of p" + partition)
                            .isEqualTo(-1L);
                });
    }
}
