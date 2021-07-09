package com.github.bsideup.liiklus.plugins.example;

import com.github.bsideup.liiklus.protocol.ReceiveReply.Record;
import com.github.bsideup.liiklus.plugins.example.support.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SmokeTest extends AbstractIntegrationTest {

    @Test
    void testPreProcessor() {
        String key = UUID.randomUUID().toString();

        publishRecord(key, "Hello!");

        Record record = receiveRecords(key).blockFirst(Duration.ofSeconds(10));

        assertThat(record).isNotNull().satisfies(it -> {
            assertThat(it.getValue().toStringUtf8()).isEqualTo("!olleH");
        });
    }

    @Test
    void testPostProcessor() {
        String key = "maskMe";

        publishRecord(key, "Hello!");

        Record record = receiveRecords(key).blockFirst(Duration.ofSeconds(10));

        assertThat(record).isNotNull().satisfies(it -> {
            assertThat(it.getValue().toStringUtf8()).isEqualTo("**masked**");
        });
    }
}
