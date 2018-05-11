package com.github.bsideup.liiklus.records;

import com.github.bsideup.liiklus.records.RecordsStorage.Envelope;

import java.util.concurrent.CompletionStage;

public interface RecordPreProcessor {

    CompletionStage<Envelope> preProcess(Envelope envelope);
}
