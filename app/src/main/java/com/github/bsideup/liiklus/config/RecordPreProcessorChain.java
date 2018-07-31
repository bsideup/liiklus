package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.records.RecordPreProcessor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.Collection;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class RecordPreProcessorChain {

    Collection<RecordPreProcessor> processors;

    public Iterable<RecordPreProcessor> getAll() {
        return processors;
    }
}
