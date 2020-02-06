package com.github.bsideup.liiklus.records;

import io.cloudevents.CloudEvent;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.extensions.InMemoryFormat;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import io.cloudevents.v1.ContextAttributes;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Getter
@ToString
@AllArgsConstructor
@FieldDefaults(makeFinal = true)
@With
public class LiiklusCloudEvent implements CloudEvent<LiiklusAttributes, ByteBuffer>, LiiklusAttributes {

    static final String HEADER_PREFIX = "ce_";

    static LiiklusCloudEvent of(ByteBuffer data, Map<String, String> headers) {
        Map<String, String> rawExtensions = new HashMap<>();

        String[] id = new String[1];
        String[] type = new String[1];
        String[] rawSource = new String[1];
        String[] mediaType = new String[1];
        String[] rawTime = new String[1];

        headers.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            key = key.toLowerCase(Locale.US);

            if (key.startsWith(HEADER_PREFIX)) {
                key = key.substring(HEADER_PREFIX.length());
            }

            if (ContextAttributes.id.name().equals(key)) {
                id[0] = value;
            } else if (ContextAttributes.type.name().equals(key)) {
                type[0] = value;
            } else if (ContextAttributes.source.name().equals(key)) {
                rawSource[0] = value;
            } else if (ContextAttributes.datacontenttype.name().equals(key)) {
                mediaType[0] = value;
            } else if (ContextAttributes.time.name().equals(key)) {
                rawTime[0] = value;
            } else if (ContextAttributes.specversion.name().equals(key)) {
                // Ignore
            } else {
                rawExtensions.put(key, value);
            }
        });

        return new LiiklusCloudEvent(
                id[0],
                type[0],
                rawSource[0],
                mediaType[0],
                rawTime[0],
                data,
                rawExtensions
        );
    }

    static byte[] asBytes(@NonNull ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.duplicate().get(bytes);
        return bytes;
    }

    String specversion = "1.0";

    @NonNull
    String id;

    @NonNull
    String type;

    @NonNull
    String rawSource;

    String mediaType;

    String rawTime;

    ByteBuffer data;

    Map<String, String> rawExtensions;

    @Getter(lazy = true)
    @Deprecated
    private URI source = rawSource == null ? null : URI.create(rawSource);

    @Getter(lazy = true)
    @Deprecated
    private ZonedDateTime time = rawTime == null ? null : ZonedDateTime.parse(rawTime);

    @Getter(lazy = true)
    private byte[] dataBase64 = data == null ? null : asBytes(data);

    public Map<String, String> getHeaders() {
        Map<String, String> result = new HashMap<>();
        result.putAll(rawExtensions);

        result.put(HEADER_PREFIX + ContextAttributes.specversion.name(), specversion);
        result.put(HEADER_PREFIX + ContextAttributes.id.name(), id);
        result.put(HEADER_PREFIX + ContextAttributes.type.name(), type);

        if (rawSource != null) {
            result.put(HEADER_PREFIX + ContextAttributes.source.name(), rawSource);
        }
        if (mediaType != null) {
            result.put(HEADER_PREFIX + ContextAttributes.datacontenttype.name(), mediaType);
        }
        if (rawTime != null) {
            result.put(HEADER_PREFIX + ContextAttributes.time.name(), rawTime);
        }

        return result;
    }

    public void forEachHeader(BiConsumer<String, String> consumer) {
        // TODO optimize
        getHeaders().forEach(consumer);
    }

    CloudEventImpl<ByteBuffer> toV1() {
        Map<String, String> rawExtensions = this.rawExtensions;
        return CloudEventBuilder.<ByteBuffer>builder()
                .withId(id)
                .withType(type)
                .withSource(getSource())
                .withDataContentType(mediaType)
                .withTime(getTime())
                .withExtension(new ExtensionFormat() {
                    @Override
                    public InMemoryFormat memory() {
                        return new InMemoryFormat() {
                            @Override
                            public String getKey() {
                                return "liiklusKV";
                            }

                            @Override
                            public Object getValue() {
                                return rawExtensions;
                            }

                            @Override
                            public Class<?> getValueType() {
                                return Map.class;
                            }
                        };
                    }

                    @Override
                    public Map<String, String> transport() {
                        return rawExtensions;
                    }
                })
                .withData(data)
                .build();
    }

    @Override
    public LiiklusAttributes getAttributes() {
        // Save 1 allocation
        return this;
    }

    public ByteBuffer getDataOrNull() {
        return data;
    }

    @Override
    public Optional<ByteBuffer> getData() {
        return Optional.ofNullable(data);
    }

    @Override
    public Map<String, Object> getExtensions() {
        // TODO there must be a better way
        return rawExtensions.entrySet().stream().collect(Collectors.toMap(
                it -> it.getKey(),
                it -> KeyValueExtension.of(it.getKey(), it.getValue())
        ));
    }

    public String getMediaTypeOrNull() {
        return mediaType;
    }

    @Override
    public Optional<String> getMediaType() {
        return Optional.ofNullable(mediaType);
    }

    public ByteBuffer asJson() {
        // TODO
        return ByteBuffer.wrap(Json.binaryEncode(toV1())).asReadOnlyBuffer();
    }
}

