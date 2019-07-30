package com.github.bsideup.liiklus.pulsar;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.pulsar.container.PulsarTlsContainer;
import com.github.bsideup.liiklus.records.RecordStorageTests;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.junit.jupiter.api.AfterAll;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;

import java.util.Map;

public class PulsarWithTlsRecordsStorageTest extends PulsarAbstractStorageTest implements RecordStorageTests {

    private static final PulsarTlsContainer pulsar = new PulsarTlsContainer(VERSION).withTlsAuthentication();

    static {
        pulsar.start();

        System.getProperties().putAll(Map.of(
                "pulsar.serviceUrl", pulsar.getPulsarTlsBrokerUrl(),
                "pulsar.tlsTrustCertsFilePath", pulsar.getCaCert().toAbsolutePath().toString()
        ));
        applicationContext = new ApplicationRunner("PULSAR", "MEMORY").run();
    }

    @Getter
    RecordsStorage target = applicationContext.getBean(RecordsStorage.class);

    @SneakyThrows
    public PulsarWithTlsRecordsStorageTest() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpsServiceUrl())
                .tlsTrustCertsFilePath(pulsar.getCaCert().toAbsolutePath().toString())
                .authentication(new AuthenticationTls(
                        pulsar.getUserCert().toAbsolutePath().toString(),
                        pulsar.getUserKey().toAbsolutePath().toString()
                ))
                .build();

        pulsarAdmin.topics().createPartitionedTopic(topic, getNumberOfPartitions());
    }

    @AfterAll
    static void tearDown() {
        SpringApplication.exit(applicationContext, (ExitCodeGenerator) () -> 0);
        pulsar.stop();
    }
}