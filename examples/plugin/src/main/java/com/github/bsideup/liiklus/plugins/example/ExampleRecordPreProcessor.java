package com.github.bsideup.liiklus.plugins.example;

import com.github.bsideup.liiklus.records.LiiklusCloudEvent;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ExampleRecordPreProcessor implements RecordPreProcessor {

    @Override
    public CompletionStage<Envelope> preProcess(Envelope envelope) {
        LiiklusCloudEvent event;
        if (envelope.getRawValue() instanceof LiiklusCloudEvent) {
            event = (LiiklusCloudEvent) envelope.getRawValue();
        } else {
            CompletableFuture<Envelope> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(new IllegalArgumentException("raw value have to be LiiklusCloudEvent, got: " + envelope.getRawValue().getClass().getName()));
            return completableFuture;
        }

        String value = event.getData().map(StandardCharsets.UTF_8::decode).orElse(CharBuffer.wrap("empty-string")).toString();

        String reversed = StringUtils.reverse(value);
        return CompletableFuture.completedFuture(
                envelope.withValue(
                        new LiiklusCloudEvent(
                                event.getId(),
                                event.getType(),
                                event.getRawSource(),
                                event.getMediaTypeOrNull(),
                                event.getRawTime(),
                                ByteBuffer.wrap(reversed.getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer(),
                                new HashMap<>()
                        ),
                        LiiklusCloudEvent::asJson
                )
        );
    }
}
