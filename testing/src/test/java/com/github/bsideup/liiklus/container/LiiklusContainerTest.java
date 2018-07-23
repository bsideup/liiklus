package com.github.bsideup.liiklus.container;

import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import static org.junit.Assert.fail;

public class LiiklusContainerTest {

    static KafkaContainer kafka = new KafkaContainer();

    static {
        kafka.start();
    }

    @Test
    public void shouldStart() {
        try (LiiklusContainer liiklusContainer = new LiiklusContainer("0.4.5")) {

            liiklusContainer.withKafka(kafka).start();

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}