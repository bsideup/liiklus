package com.github.bsideup.liiklus.plugins.example;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage.Record;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ExampleRecordPostProcessor implements RecordPostProcessor {
    @Override
    public Publisher<Record> postProcess(Publisher<Record> publisher) {
        return Flux.from(publisher)
                .map(record -> {
                    String key = StandardCharsets.UTF_8.decode(record.getEnvelope().getKey().duplicate()).toString();
                    if ("maskMe".equals(key)) {
                        return new Record(
                                record.getEnvelope().withValue(ByteBuffer.wrap("**masked**".getBytes())),
                                record.getTimestamp(),
                                record.getPartition(),
                                record.getOffset()
                        );
                    } else {
                        return record;
                    }
                });
    }
}
