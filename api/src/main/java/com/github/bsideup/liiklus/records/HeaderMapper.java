package com.github.bsideup.liiklus.records;

import io.cloudevents.v1.ContextAttributes;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static com.github.bsideup.liiklus.records.AttributeMapper.HEADER_PREFIX;

/**
 * See https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md
 */
class HeaderMapper {

    static final String CONTENT_TYPE = "content-type";

    public static Map<String, String> map(
            @NonNull Map<String, String> attributes,
            @NonNull Map<String, String> extensions
    ) {
        Map<String, String> result = new HashMap<>();

        extensions.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            result.put(key, value);
        });

        attributes.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            key = key.toLowerCase(Locale.US);

            if (ContextAttributes.datacontenttype.name().equals(key)) {
                key = CONTENT_TYPE;
            } else {
                key = HEADER_PREFIX + key;
            }

            result.put(key, value);
        });

        return result;
    }
}
