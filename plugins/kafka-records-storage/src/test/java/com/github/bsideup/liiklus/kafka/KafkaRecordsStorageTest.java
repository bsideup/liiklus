package com.github.bsideup.liiklus.kafka;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.val;
import org.junit.Test;
import reactor.kafka.sender.KafkaSender;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class KafkaRecordsStorageTest {

    KafkaRecordsStorage target = new KafkaRecordsStorage(
            "localhost:9092",
            mock(PositionsStorage.class),
            mock(KafkaSender.class)
    );

    @Test
    public void testGroup() {
        val options = target.createReceiverOptions("myGroup", Optional.empty(), Optional.empty());

        assertThat(options.groupId())
                .describedAs("groupId")
                .isEqualTo("myGroup");
    }

    @Test
    public void testGroupWithVersion() {
        val options = target.createReceiverOptions("myGroup", Optional.of(3), Optional.empty());

        assertThat(options.groupId())
                .describedAs("groupId")
                .isEqualTo("myGroup-v3");
    }
}