package com.github.bsideup.liiklus.test;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.Getter;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ProcessorPluginMock implements RecordPreProcessor, RecordPostProcessor {

    @Getter
    List<RecordPreProcessor> preProcessors = new ArrayList<>();

    @Getter
    List<RecordPostProcessor> postProcessors = new ArrayList<>();

    public ProcessorPluginMock() {
    }

    @Override
    public CompletionStage<RecordsStorage.Envelope> preProcess(RecordsStorage.Envelope envelope) {
        var future = CompletableFuture.completedFuture(envelope);
        for (RecordPreProcessor preProcessor : preProcessors) {
            future = future.thenComposeAsync(preProcessor::preProcess);
        }
        return future;
    }

    @Override
    public Publisher<RecordsStorage.Record> postProcess(Publisher<RecordsStorage.Record> records) {
        for (RecordPostProcessor postProcessor : postProcessors) {
            records = postProcessor.postProcess(records);
        }
        return records;
    }
}
