package com.github.bsideup.liiklus.records;

import io.cloudevents.v1.ContextAttributes;
import lombok.NonNull;

import java.util.*;

/**
 * See https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md
 */
class AttributeMapper {

    static final String HEADER_PREFIX = "ce_";

    public static Map<String, String> map(@NonNull Map<String, String> headers) {
        Map<String, String> result = new HashMap<>();

        headers.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            key = key.toLowerCase(Locale.US);

            if (HeaderMapper.CONTENT_TYPE.equals(key)) {
                key = ContextAttributes.datacontenttype.name();
            } else {
                if (!(key.startsWith(HEADER_PREFIX))) {
                    return;
                }
                key = key.substring(HEADER_PREFIX.length());
            }

            result.put(key, value);
        });

        return result;
    }
}
