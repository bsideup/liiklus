package com.github.bsideup.liiklus.records;

import org.assertj.core.data.MapEntry;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * See <a href="https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md">Kafka Protocol Binding</a>
 */
@RunWith(Enclosed.class)
public class LiiklusCloudEventTest {

    public static class Attributes {

        @Test
        public void serialization() {
            LiiklusCloudEvent event = newBuilder()
                    .mediaType("text/plain")
                    .build();

            assertThat(event.getHeaders()).containsOnly(
                    entry("ce_specversion", "1.0"),
                    entry("ce_id", event.getId()),
                    entry("ce_type", event.getType()),
                    entry("ce_source", event.getRawSource()),
                    entry("ce_datacontenttype", "text/plain")
            );
        }

        @Test
        public void deserialization() {
            String id = UUID.randomUUID().toString();
            Map<String, String> headers = Stream.of(
                    entry("ce_specversion", "1.0"),
                    entry("ce_id", id),
                    entry("ce_type", "com.example.event"),
                    entry("ce_source", "/tests"),
                    entry("ce_datacontenttype", "text/plain")
            ).collect(Collectors.toMap(MapEntry::getKey, MapEntry::getValue));

            LiiklusCloudEvent event = LiiklusCloudEvent.of(null, headers);
            assertThat(event)
                    .returns("1.0", LiiklusCloudEvent::getSpecversion)
                    .returns(id, LiiklusCloudEvent::getId)
                    .returns("com.example.event", LiiklusCloudEvent::getType)
                    .returns("/tests", LiiklusCloudEvent::getRawSource)
                    .returns("text/plain", LiiklusCloudEvent::getMediaTypeOrNull);
        }
    }

    public static class Extensions {

        @Test
        public void serialization() {
            LiiklusCloudEvent event = newBuilder()
                    .rawExtension("comexamplefoo", "foo")
                    .rawExtension("comexamplebar", "bar")
                    .build();

            assertThat(event.getHeaders()).contains(
                    entry("ce_comexamplefoo", "foo"),
                    entry("ce_comexamplebar", "bar")
            );
        }

        @Test
        public void deserialization() {
            String id = UUID.randomUUID().toString();
            Map<String, String> headers = Stream.of(
                    entry("ce_specversion", "1.0"),
                    entry("ce_id", id),
                    entry("ce_type", "com.example.event"),
                    entry("ce_source", "/tests"),
                    entry("ce_comexamplefoo", "foo"),
                    entry("ce_comexamplebar", "bar")
            ).collect(Collectors.toMap(MapEntry::getKey, MapEntry::getValue));

            LiiklusCloudEvent event = LiiklusCloudEvent.of(null, headers);
            assertThat(event.getRawExtensions()).containsOnly(
                    entry("comexamplefoo", "foo"),
                    entry("comexamplebar", "bar")
            );
        }
    }

    private static LiiklusCloudEvent.LiiklusCloudEventBuilder newBuilder() {
        return LiiklusCloudEvent.builder()
                .id(UUID.randomUUID().toString())
                .type("com.example.event")
                .rawSource("/example");
    }
}