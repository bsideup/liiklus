package com.github.bsideup.liiklus.schema;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.service.LiiklusService;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static com.github.bsideup.liiklus.schema.SchemaPluginConfigurationTest.getSchemaURL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SmokeTest {

    private static ConfigurableApplicationContext APP;

    @BeforeAll
    static void beforeAll() {
        APP = new ApplicationRunner("MEMORY", "MEMORY")
                .withProperty("schema.enabled", true)
                .withProperty("schema.schemaURL", getSchemaURL())
                .run();
    }

    @AfterAll
    static void afterAll() {
        APP.close();
    }

    @Test
    void validEvent() {
        var event = LiiklusEvent.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setType("com.example.cloudevent")
                .setSource("/tests")
                .setDataContentType("application/json")
                .setData(ByteString.copyFromUtf8("{\"foo\":\"Hello!\"}"))
                .build();

        send(event);
    }

    @Test
    void missingBody() {
        var event = LiiklusEvent.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setType("com.example.cloudevent")
                .setSource("/tests")
                .setDataContentType("application/json")
                .build();

        assertThatThrownBy(() -> send(event)).hasMessageContaining("object expected");
    }

    @Test
    void invalidData() {
        var event = LiiklusEvent.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setType("com.example.cloudevent")
                .setSource("/tests")
                .setDataContentType("application/json")
                .setData(ByteString.copyFromUtf8("wtf"))
                .build();

        assertThatThrownBy(() -> send(event)).hasCauseInstanceOf(JsonParseException.class);
    }

    private void send(LiiklusEvent event) {
        APP.getBean(LiiklusService.class).publish(Mono.just(
                PublishRequest.newBuilder()
                        .setTopic("events")
                        .setLiiklusEvent(event)
                        .build()
        )).block();
    }
}
