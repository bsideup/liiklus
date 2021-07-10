package com.github.bsideup.liiklus.container;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class LiiklusContainerTest {

    static final String LATEST_VERSION = "0.10.0-rc1";
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
            .withNetwork(Network.newNetwork());

    static {
        kafka.start();
    }

    @Test
    void shouldStartWithKafkaRecordStorage() {
        try (LiiklusContainer liiklusContainer = new LiiklusContainer(LATEST_VERSION)) {
            liiklusContainer.withKafka(kafka).start();
        }
    }

    @Test
    void shouldStartDefaultMemoryRecordStorage() {
        try (LiiklusContainer liiklusContainer = new LiiklusContainer(LATEST_VERSION)) {
            liiklusContainer.start();
        }
    }
}