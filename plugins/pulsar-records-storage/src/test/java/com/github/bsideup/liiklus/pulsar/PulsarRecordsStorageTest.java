package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.AfterAll;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.testcontainers.containers.PulsarContainer;

public class PulsarRecordsStorageTest extends PulsarAbstractStorageTest implements RecordStorageTests {

    private static final PulsarContainer pulsar = new PulsarContainer(VERSION);

    static {
        pulsar.start();
        System.setProperty("pulsar.serviceUrl", pulsar.getPulsarBrokerUrl());

        applicationContext = new ApplicationRunner("PULSAR", "MEMORY").run();
    }

    @Getter
    RecordsStorage target = applicationContext.getBean(RecordsStorage.class);

    @SneakyThrows
    public PulsarRecordsStorageTest() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();

        pulsarAdmin.topics().createPartitionedTopic(topic, getNumberOfPartitions());
    }

    @AfterAll
    static void tearDown() {
        SpringApplication.exit(applicationContext, (ExitCodeGenerator) () -> 0);
        pulsar.stop();
    }
}