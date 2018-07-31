package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.Collection;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class RecordPostProcessorChain {

    Collection<RecordPostProcessor> processors;

    public Iterable<RecordPostProcessor> getAll() {
        return processors;
    }
}
