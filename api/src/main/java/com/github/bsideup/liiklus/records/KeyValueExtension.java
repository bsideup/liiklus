package com.github.bsideup.liiklus.records;

import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.extensions.InMemoryFormat;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

@Value
@RequiredArgsConstructor(staticName = "of")
public class KeyValueExtension implements ExtensionFormat, InMemoryFormat {

    String key;

    String value;

    @Override
    public InMemoryFormat memory() {
        return this;
    }

    @Override
    public Map<String, String> transport() {
        return Collections.singletonMap(key, value);
    }

    @Override
    public Class<?> getValueType() {
        return String.class;
    }
}
