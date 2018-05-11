package com.github.bsideup.liiklus.records;

import com.github.bsideup.liiklus.records.RecordsStorage.Record;
import org.reactivestreams.Publisher;

public interface RecordPostProcessor {

    Publisher<Record> postProcess(Publisher<Record> records);
}
