package com.github.bsideup.liiklus.container;

import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

public class LiiklusContainerTest {

    static final String LATEST_VERSION = "0.7.0";
    @SuppressWarnings("deprecation")
    static KafkaContainer kafka = new KafkaContainer();

    static {
        kafka.start();
    }

    @Test
    public void shouldStartWithKafkaRecordStorage() {
        try (LiiklusContainer liiklusContainer = new LiiklusContainer(LATEST_VERSION)) {

            liiklusContainer.withKafka(kafka).start();
        }
    }

    @Test
    public void shouldStartDefaultMemoryRecordStorage() {
        try (LiiklusContainer liiklusContainer = new LiiklusContainer(LATEST_VERSION)) {

            liiklusContainer.start();
        }
    }
}