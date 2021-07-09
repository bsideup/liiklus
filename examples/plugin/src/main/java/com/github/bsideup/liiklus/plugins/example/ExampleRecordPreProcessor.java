package com.github.bsideup.liiklus.plugins.example;

import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ExampleRecordPreProcessor implements RecordPreProcessor {

    @Override
    public CompletionStage<Envelope> preProcess(Envelope envelope) {
        String value = StandardCharsets.UTF_8.decode(envelope.getValue()).toString();

        String reversed = StringUtils.reverse(value);
        return CompletableFuture.completedFuture(
                envelope.withValue(ByteBuffer.wrap(reversed.getBytes()))
        );
    }
}
