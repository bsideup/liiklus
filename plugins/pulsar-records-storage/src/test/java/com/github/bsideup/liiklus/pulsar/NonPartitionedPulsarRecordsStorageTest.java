package com.github.bsideup.liiklus.pulsar;

import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;

public class NonPartitionedPulsarRecordsStorageTest extends AbstractPulsarRecordsStorageTest {

    @SneakyThrows
    public NonPartitionedPulsarRecordsStorageTest() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();

        pulsarAdmin.topics().createNonPartitionedTopic(topic);
    }

    @Override
    public String keyByPartition(int partition) {
        return "foo";
    }

    @Override
    public int getNumberOfPartitions() {
        return 1;
    }
}